"""
Motor de OCR de alta performance para PDFs.

Otimizado para Intel Xeon E5-2620 v4 (8 núcleos / 16 threads @ 2.10GHz).
Usa ProcessPoolExecutor com 14 workers para paralelização massiva,
pipeline direto pdftoppm → tesseract → ghostscript (merge),
e RAM Disk em /mnt/ramdisk para I/O zero-latência.

Variável OMP_THREAD_LIMIT=1 deve ser definida ANTES de qualquer import
(já feito em app.py) para evitar contenção de threads do Tesseract.
"""

import os
import uuid
import shutil
import logging
import subprocess
import concurrent.futures
from pathlib import Path

# ── Configurações ────────────────────────────────────────────────────────────
MAX_WORKERS = 14             # 14 de 16 threads → reserva 2 para OS/Flask
RAMDISK_BASE = "/mnt/ramdisk"  # RAM Disk primário
RAMDISK_FALLBACK = "/dev/shm"  # Fallback padrão Linux
TESSERACT_LANG = "por+eng"
TESSERACT_DPI = 300
GS_RENDERING_THREADS = 16
GS_BUFFER_SPACE = 1_000_000_000  # 1 GB de buffer para o Ghostscript
PDFTOPPM_FORMAT = "png"        # PNG sem perda para máxima qualidade OCR
# ─────────────────────────────────────────────────────────────────────────────


def _get_ramdisk_dir() -> str:
    """Retorna o melhor diretório de RAM Disk disponível, criando subpasta isolada."""
    for base in (RAMDISK_BASE, RAMDISK_FALLBACK):
        if os.path.isdir(base) and os.access(base, os.W_OK):
            work_dir = os.path.join(base, "pdf-optimizer-hp")
            os.makedirs(work_dir, exist_ok=True)
            logging.info(f"[HP-OCR] RAM Disk selecionado: {work_dir}")
            return work_dir

    # Último recurso: /tmp (disco, mas garante que o código funciona)
    import tempfile
    fallback = os.path.join(tempfile.gettempdir(), "pdf-optimizer-hp")
    os.makedirs(fallback, exist_ok=True)
    logging.warning(f"[HP-OCR] RAM Disk indisponível. Usando fallback: {fallback}")
    return fallback


def _get_page_count(pdf_path: str) -> int:
    """Obtém o número de páginas via Ghostscript (não depende de PyMuPDF)."""
    gs = _locate_gs()
    result = subprocess.run(
        [gs, "-q", "-dNODISPLAY", "-dNOSAFER",
         "-c", f"({pdf_path}) (r) file runpdfbegin pdfpagecount = quit"],
        capture_output=True, text=True, timeout=60
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Ghostscript falhou ao contar páginas: {result.stderr.strip()}"
        )
    return int(result.stdout.strip())


def _locate_gs() -> str:
    """Localiza o executável do Ghostscript."""
    gs = shutil.which("gs") or shutil.which("gswin64c") or shutil.which("gswin32c")
    if not gs:
        raise FileNotFoundError(
            "Ghostscript não encontrado no PATH. Instale com: apt install ghostscript"
        )
    return gs


def _convert_page_to_image(pdf_path: str, page_num: int, output_prefix: str) -> str:
    """
    Converte uma única página do PDF em imagem PNG via pdftoppm.
    page_num é 1-indexed (padrão pdftoppm).
    Retorna o caminho da imagem gerada.
    """
    cmd = [
        "pdftoppm",
        "-f", str(page_num),
        "-l", str(page_num),
        "-r", str(TESSERACT_DPI),
        "-png",
        "-singlefile",
        pdf_path,
        output_prefix,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            f"pdftoppm falhou na página {page_num}: {result.stderr.strip()}"
        )

    expected = f"{output_prefix}.png"
    if not os.path.isfile(expected):
        raise FileNotFoundError(
            f"pdftoppm não gerou a imagem esperada: {expected}"
        )
    return expected


def _ocr_page(args: tuple) -> dict:
    """
    Worker executado em processo separado (ProcessPoolExecutor).
    Recebe (pdf_path, page_num, work_dir) e retorna dict com resultado.

    Pipeline por página:
        1. pdftoppm  → PNG (imagem da página)
        2. tesseract → PDF pesquisável (OCR)

    Todo I/O acontece no RAM Disk para latência mínima.
    """
    pdf_path, page_num, work_dir = args
    uid = uuid.uuid4().hex[:12]
    img_prefix = os.path.join(work_dir, f"page_{page_num:05d}_{uid}")
    ocr_output_base = os.path.join(work_dir, f"ocr_{page_num:05d}_{uid}")
    ocr_output_pdf = f"{ocr_output_base}.pdf"
    img_path = None

    try:
        # ── 1. Converter página para PNG ─────────────────────────────────
        img_path = _convert_page_to_image(pdf_path, page_num, img_prefix)

        # ── 2. OCR com Tesseract → PDF pesquisável ──────────────────────
        cmd_tess = [
            "tesseract",
            img_path,
            ocr_output_base,          # sem extensão; tesseract adiciona .pdf
            "-l", TESSERACT_LANG,
            "--dpi", str(TESSERACT_DPI),
            "--psm", "3",              # Automatic page segmentation
            "pdf",                     # Output format
        ]
        result = subprocess.run(cmd_tess, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            raise RuntimeError(
                f"Tesseract falhou na página {page_num}: {result.stderr.strip()}"
            )

        if not os.path.isfile(ocr_output_pdf):
            raise FileNotFoundError(
                f"Tesseract não gerou PDF: {ocr_output_pdf}"
            )

        return {
            "page": page_num,
            "pdf": ocr_output_pdf,
            "success": True,
            "error": None,
        }

    except Exception as e:
        logging.error(f"[HP-OCR] Erro na página {page_num}: {e}")
        return {
            "page": page_num,
            "pdf": None,
            "success": False,
            "error": str(e),
        }

    finally:
        # Limpa imagem temporária (o PDF OCR será limpo após o merge)
        if img_path and os.path.isfile(img_path):
            try:
                os.remove(img_path)
            except OSError:
                pass


def _merge_pdfs_ghostscript(pdf_fragments: list, output_path: str) -> None:
    """
    Merge final de todos os fragmentos PDF usando Ghostscript com
    flags de alta performance para o Xeon.
    """
    gs = _locate_gs()
    cmd = [
        gs,
        "-dBATCH",
        "-dNOPAUSE",
        "-dQUIET",
        "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/ebook",
        f"-dNumRenderingThreads={GS_RENDERING_THREADS}",
        f"-dBufferSpace={GS_BUFFER_SPACE}",
        "-dAutoRotatePages=/None",
        "-dColorImageDownsampleType=/Bicubic",
        "-dGrayImageDownsampleType=/Bicubic",
        f"-sOutputFile={output_path}",
    ] + pdf_fragments

    logging.info(
        f"[HP-OCR] Ghostscript merge: {len(pdf_fragments)} fragmentos → {output_path}"
    )
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(
            f"Ghostscript merge falhou (exit {result.returncode}): "
            f"{result.stderr.strip()}"
        )


def _cleanup_work_dir(work_dir: str) -> None:
    """Remove o diretório de trabalho e todo seu conteúdo."""
    if not work_dir or not os.path.isdir(work_dir):
        return
    try:
        shutil.rmtree(work_dir, ignore_errors=True)
        logging.info(f"[HP-OCR] Diretório temporário removido: {work_dir}")
    except Exception as e:
        logging.warning(f"[HP-OCR] Falha ao limpar {work_dir}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  FUNÇÃO PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def process_pdf_high_performance(input_path: str, callback=None) -> str:
    """
    Processa um PDF com OCR de alta performance usando paralelização massiva.

    Pipeline:
        1. Conta páginas do PDF de entrada
        2. Converte cada página para PNG (pdftoppm) em paralelo
        3. Executa OCR (tesseract) em cada imagem em paralelo
        4. Faz merge final (ghostscript) com flags otimizadas para Xeon

    Args:
        input_path: Caminho absoluto do PDF de entrada.
        callback:   Função opcional callback(current_page, total_pages)
                    chamada após cada página processada.

    Returns:
        Caminho absoluto do PDF final otimizado (no diretório do input).

    Raises:
        FileNotFoundError: Se o arquivo de entrada não existir.
        RuntimeError:      Se o processamento falhar criticamente.
    """
    input_path = os.path.abspath(input_path)
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")

    # ── Diretório de trabalho isolado no RAM Disk ────────────────────────
    session_id = uuid.uuid4().hex[:16]
    ramdisk_base = _get_ramdisk_dir()
    work_dir = os.path.join(ramdisk_base, f"session_{session_id}")
    os.makedirs(work_dir, exist_ok=True)

    # ── Caminho de saída (mesmo diretório do input) ─────────────────────
    input_stem = Path(input_path).stem
    input_dir = os.path.dirname(input_path)
    output_path = os.path.join(input_dir, f"{input_stem}_hp_ocr.pdf")

    logging.info(
        f"[HP-OCR] Início | input={input_path} | workers={MAX_WORKERS} | "
        f"ramdisk={work_dir}"
    )

    try:
        # ── 1. Contagem de páginas ──────────────────────────────────────
        total_pages = _get_page_count(input_path)
        logging.info(f"[HP-OCR] Total de páginas: {total_pages}")

        if total_pages == 0:
            shutil.copy2(input_path, output_path)
            logging.warning("[HP-OCR] PDF com 0 páginas. Retornando cópia.")
            return output_path

        # ── 2. Processamento paralelo (pdftoppm + tesseract) ────────────
        tasks = [
            (input_path, page_num, work_dir)
            for page_num in range(1, total_pages + 1)
        ]

        results = [None] * total_pages  # Slot por página (ordem preservada)
        completed = 0
        failed_pages = []

        with concurrent.futures.ProcessPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:
            future_to_page = {
                executor.submit(_ocr_page, task): task[1]
                for task in tasks
            }

            for future in concurrent.futures.as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    result = future.result(timeout=300)
                    results[page_num - 1] = result

                    if not result["success"]:
                        failed_pages.append(page_num)
                        logging.warning(
                            f"[HP-OCR] Página {page_num} falhou: {result['error']}"
                        )
                except Exception as e:
                    failed_pages.append(page_num)
                    results[page_num - 1] = {
                        "page": page_num,
                        "pdf": None,
                        "success": False,
                        "error": str(e),
                    }
                    logging.error(
                        f"[HP-OCR] Exceção no worker da página {page_num}: {e}"
                    )
                finally:
                    completed += 1
                    if callback:
                        try:
                            callback(completed, total_pages)
                        except Exception:
                            pass

        # ── 3. Coleta dos fragmentos bem-sucedidos (na ordem) ───────────
        pdf_fragments = []
        for r in results:
            if r and r["success"] and r["pdf"] and os.path.isfile(r["pdf"]):
                pdf_fragments.append(r["pdf"])

        if not pdf_fragments:
            raise RuntimeError(
                f"Nenhuma página foi processada com sucesso. "
                f"Falhas: {len(failed_pages)}/{total_pages}"
            )

        if failed_pages:
            logging.warning(
                f"[HP-OCR] {len(failed_pages)} página(s) falharam e foram "
                f"omitidas: {failed_pages}"
            )

        # ── 4. Merge final com Ghostscript ──────────────────────────────
        # Merge intermediário no RAM Disk, depois move para destino final
        merged_tmp = os.path.join(work_dir, "merged_final.pdf")
        _merge_pdfs_ghostscript(pdf_fragments, merged_tmp)

        if not os.path.isfile(merged_tmp):
            raise RuntimeError("Ghostscript não gerou o arquivo de merge final.")

        # Move do RAM Disk para o destino final (disco persistente)
        shutil.move(merged_tmp, output_path)

        final_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logging.info(
            f"[HP-OCR] Concluído | {total_pages} páginas | "
            f"{len(failed_pages)} falhas | "
            f"output={output_path} ({final_size_mb:.2f} MB)"
        )

        return output_path

    except Exception:
        # Re-raise após garantir cleanup
        logging.exception("[HP-OCR] Falha crítica no processamento")
        raise

    finally:
        # ── Limpeza garantida do RAM Disk ───────────────────────────────
        _cleanup_work_dir(work_dir)
