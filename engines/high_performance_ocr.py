"""
Motor de OCR de alta performance para PDFs.

Otimizado para Intel Xeon E5-2620 v4 (8 núcleos / 16 threads @ 2.10GHz).
Usa ProcessPoolExecutor com 14 workers para paralelização massiva,
pipeline direto pikepdf → fitz → tesseract → merge com ghostscript,
e RAM Disk em /mnt/ramdisk para I/O zero-latência.

Variável OMP_THREAD_LIMIT=1 deve ser definida ANTES de qualquer import
(já feito em app.py) para evitar contenção de threads do Tesseract.

Compressão por página:
  Páginas acima de 500 KB são rasterizadas via pdftoppm → PIL (JPEG) → pikepdf.
  Isso elimina vetores pesados gerados pelo Tesseract e reduz ~73% em scans.
"""

import os
import io
import uuid
import shutil
import logging
import subprocess
import concurrent.futures
import time
from decimal import Decimal
from pathlib import Path

import pikepdf
from PIL import Image
import fitz  # PyMuPDF — usado apenas em fragmentos isolados (1 página cada)

from engines.constants import (
    MAX_WORKERS,
    TESSERACT_LANG,
    TESSERACT_DPI,
    GS_RENDERING_THREADS,
    GS_BUFFER_SPACE,
    PAGE_SIZE_LIMIT,
    PAGE_SIZE_LIMIT_SPLIT,
    DPI_PASSES_NORMAL,
    DPI_PASSES_SPLIT,
    EXTRA_COMPRESS_THRESHOLD_MB,
    MIN_IMAGE_AREA_RATIO,
    MIN_PDF_SIZE_BYTES,
    MIN_RAMDISK_SPACE_MB,
    PNG_VALIDATION_TIMEOUT,
    PDF_VALIDATION_TIMEOUT,
)

# ── Configurações Locais ─────────────────────────────────────────────────────
RAMDISK_BASE     = "/mnt/ramdisk"   # RAM Disk primário
RAMDISK_FALLBACK = "/dev/shm"       # Fallback padrão Linux

# Aliases para compatibilidade interna
_DPI_PASSES       = DPI_PASSES_NORMAL
_DPI_PASSES_SPLIT = DPI_PASSES_SPLIT
# Preset equivalente ao modo 72 DPI (/screen, qfactor ~0.45).
_EXTRA_SELECTED_DPI_72 = 72
_EXTRA_SELECTED_QUALITY_72 = 45
# ─────────────────────────────────────────────────────────────────────────────


def _mode(values: list[int]) -> int:
    """Retorna o valor mais frequente da lista."""
    return max(set(values), key=values.count)


def _resolve_gs_compression(level: int | None) -> dict:
    """
    Resolve configuração de compressão GS a partir do nível informado.

    Suporta:
      - Presets do front (1..7)
      - DPI direto (ex.: 150, 70)
    """
    default_level = 3
    val = default_level if level is None else int(level)

    preset_to_dpi = {
        1: 200,
        2: 150,
        3: 100,
        4: 60,
        5: 50,
        6: 60,
        7: 60,
    }

    if val in preset_to_dpi:
        dpi = preset_to_dpi[val]
    elif 20 <= val <= 600:
        dpi = val
    else:
        dpi = preset_to_dpi[default_level]

    # QFactor: quanto menor, mais comprime (0.0 = máx compressão, 1.0 = mínima)
    if dpi >= 180:
        pdf_settings = "/ebook"
        qfactor = 0.85
    elif dpi >= 80:
        pdf_settings = "/screen"
        qfactor = 0.76
    else:
        pdf_settings = "/screen"
        qfactor = 0.65

    return {
        "dpi": dpi,
        "pdf_settings": pdf_settings,
        "extra_dpi": max(30, int(dpi * 0.5)),
        "qfactor": qfactor,
        "extra_qfactor": 0.50,
    }


def _get_ramdisk_dir() -> str:
    """Retorna o melhor diretório de RAM Disk disponível, criando subpasta isolada."""
    for base in (RAMDISK_BASE, RAMDISK_FALLBACK):
        if os.path.isdir(base) and os.access(base, os.W_OK):
            work_dir = os.path.join(base, "pdf-optimizer-hp")
            os.makedirs(work_dir, exist_ok=True)
            logging.info(f"[HP-OCR] RAM Disk selecionado: {work_dir}")
            return work_dir

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


def _validate_pdf_integrity(pdf_path: str, timeout: float = 5) -> bool:
    """
    Valida se um arquivo PDF é válido e não está vazio.

    Args:
        pdf_path: Caminho do PDF para validar
        timeout: Timeout para validação em segundos

    Returns:
        True se PDF é válido, False caso contrário
    """
    try:
        # Validação 1: Arquivo existe?
        if not os.path.isfile(pdf_path):
            logging.warning(f"[HP-OCR] PDF não existe: {pdf_path}")
            return False

        # Validação 2: Arquivo tem tamanho mínimo (1 KB)?
        file_size = os.path.getsize(pdf_path)
        if file_size < MIN_PDF_SIZE_BYTES:
            logging.warning(f"[HP-OCR] PDF muy pequeño ({file_size} bytes): {pdf_path}")
            return False

        # Validação 3: PDF pode ser aberto e tem páginas?
        with pikepdf.open(pdf_path) as pdf:
            page_count = len(pdf.pages)
            if page_count == 0:
                logging.warning(f"[HP-OCR] PDF vazio (0 páginas): {pdf_path}")
                return False

        return True

    except Exception as e:
        logging.warning(f"[HP-OCR] Falha ar validar PDF {pdf_path}: {e}")
        return False


def _detect_blank_pdf(pdf_path: str) -> bool:
    """
    Detecta se um PDF está em branco (sem conteúdo visual).

    Verifica:
    - Se há texto suficiente (> 10 caracteres)
    - Se há imagens/conteúdo visual
    - Se o tamanho é suspeito (muito pequeno para página com OCR)

    Args:
        pdf_path: Caminho do PDF para verificar

    Returns:
        True se PDF está em branco, False caso tenha conteúdo
    """
    try:
        with pikepdf.open(pdf_path) as pdf:
            for page in pdf.pages:
                try:
                    # Tenta extrair texto com fitz
                    with fitz.open(pdf_path) as doc:
                        page_fitz = doc[0]
                        text = page_fitz.get_text().strip()
                        images = page_fitz.get_images(full=True)

                        # Se tem texto substancial OU imagens, não está branca
                        if len(text) > 10 or images:
                            return False
                except:
                    pass

        # Se passou por todas as páginas sem encontrar conteúdo, está branca
        logging.warning(f"[HP-OCR] PDF detectado como BRANCA (sem conteúdo): {pdf_path}")
        return True

    except Exception as e:
        logging.warning(f"[HP-OCR] Falha ao detectar PDF branca: {e}")
        return False


def _validate_png_image(png_path: str) -> bool:
    """
    Valida se uma imagem PNG pode ser decodificada corretamente.

    Args:
        png_path: Caminho da imagem PNG

    Returns:
        True se PNG é válido, False caso contrário
    """
    try:
        if not os.path.isfile(png_path):
            logging.warning(f"[HP-OCR] PNG não existe: {png_path}")
            return False

        with Image.open(png_path) as img:
            img.verify()  # Valida integridade da imagem

        return True

    except Exception as e:
        logging.warning(f"[HP-OCR] PNG inválido/corrompido {png_path}: {e}")
        return False


def _validate_jpeg_bytes(jpg_bytes: bytes) -> bool:
    """
    Valida se bytes JPEG podem ser decodificados corretamente.

    Args:
        jpg_bytes: Bytes JPEG para validar

    Returns:
        True se JPEG é válido, False caso contrário
    """
    try:
        if len(jpg_bytes) < 100:  # JPEG mínimo é > 100 bytes
            logging.warning(f"[HP-OCR] JPEG muy pequeño ({len(jpg_bytes)} bytes)")
            return False

        with Image.open(io.BytesIO(jpg_bytes)) as img:
            img.verify()  # Valida integridade

        return True

    except Exception as e:
        logging.warning(f"[HP-OCR] JPEG inválido/corrompido: {e}")
        return False


def _get_ramdisk_space(work_dir: str) -> float:
    """
    Retorna espaço disponível em GB no RAM Disk.

    Args:
        work_dir: Diretório de trabalho no RAM Disk

    Returns:
        Espaço disponível em GB (float)
    """
    try:
        stat = shutil.disk_usage(work_dir)
        available_gb = stat.free / (1024 ** 3)
        return available_gb
    except Exception as e:
        logging.error(f"[HP-OCR] Falha ao verificar espaço RAM Disk: {e}")
        return 0.0


def _emit_progress(
    callback,
    current: int,
    total: int,
    stage: str = "processing",
    label: str = "Processando",
    detail: str = "",
) -> None:
    """
    Envia progresso de forma compatível com callbacks antigos.

    Novo formato preferencial: callback(dict)
    Formato legado: callback(current, total)
    """
    if not callback:
        return

    payload = {
        "current": max(0, int(current or 0)),
        "total": max(0, int(total or 0)),
        "stage": stage,
        "label": label,
        "detail": detail or "",
    }

    try:
        callback(payload)
        return
    except TypeError:
        pass
    except Exception:
        return

    try:
        callback(payload["current"], payload["total"])
    except Exception:
        pass


def _ocr_page(args: tuple) -> dict:
    """
    Worker executado em processo separado (ProcessPoolExecutor).
    Recebe (pdf_path, page_num, work_dir) e retorna dict com resultado.

    Pipeline por página:
        1. Extrai página isolada com pikepdf (seguro para acesso concorrente)
        2. PyMuPDF → PNG (imagem da página)
        3. Tesseract → PDF pesquisável

    Por que NOT OCRmyPDF:
        OCRmyPDF pode gerar PDFs vazios/brancos em cenários de alta carga ou
        quando o Tesseract retorna erros silenciosos. O pipeline manual
        fitz→PNG→tesseract é mais previsível e comprovadamente seguro.

    Todo I/O acontece no RAM Disk para latência mínima.
    """
    pdf_path, page_num, work_dir = args
    uid            = uuid.uuid4().hex[:12]
    page_pdf       = os.path.join(work_dir, f"page_{page_num:05d}_{uid}.pdf")
    img_prefix     = os.path.join(work_dir, f"img_{page_num:05d}_{uid}")
    img_path       = f"{img_prefix}.png"
    ocr_output_base = os.path.join(work_dir, f"ocr_{page_num:05d}_{uid}")
    ocr_output_pdf = f"{ocr_output_base}.pdf"
    result_pdf     = None  # Arquivo que deve ser mantido (não deletar)

    try:
        # ── 1. Extrai página isolada via pikepdf ─────────────────────────
        # pikepdf é seguro para leitura concorrente do mesmo arquivo.
        # A partir daqui cada worker opera no seu próprio fragmento.
        with pikepdf.open(pdf_path) as src:
            single = pikepdf.Pdf.new()
            single.pages.append(src.pages[page_num - 1])
            single.save(page_pdf)

        # ── 2. Converter para PNG via PyMuPDF (seguro — abre apenas fragmento) ──
        try:
            with fitz.open(page_pdf) as doc:
                page = doc[0]
                zoom = TESSERACT_DPI / 72
                mat = fitz.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                pix.save(img_path)
                if not os.path.isfile(img_path):
                    raise FileNotFoundError(f"PyMuPDF não gerou imagem: {img_path}")

                # Validar PNG antes de passar para Tesseract
                if not _validate_png_image(img_path):
                    raise RuntimeError(f"PNG gerado é inválido/corrompido: {img_path}")

        except Exception as img_err:
            raise RuntimeError(f"Falha ao gerar imagem PNG: {img_err}")

        # ── 3. OCR via Tesseract ──────────────────────────────────────────
        cmd_tess = [
            "tesseract",
            img_path,
            ocr_output_base,
            "-l", TESSERACT_LANG,
            "--dpi", str(TESSERACT_DPI),
            "--psm", "3",
            "pdf",
        ]
        result = subprocess.run(cmd_tess, capture_output=True, text=True, timeout=180)

        if result.returncode != 0:
            raise RuntimeError(
                f"Tesseract falhou na página {page_num} "
                f"(exit {result.returncode}): {result.stderr.strip()}"
            )
        if not os.path.isfile(ocr_output_pdf):
            raise FileNotFoundError(
                f"Tesseract não gerou PDF: {ocr_output_pdf}"
            )

        logging.debug(f"[HP-OCR] Página {page_num}: OCR concluído com sucesso")

        # ── 3b. Validar se PDF de OCR não saiu branco ──────────────────────
        if _detect_blank_pdf(ocr_output_pdf):
            raise RuntimeError(
                f"[HP-OCR] Página {page_num}: OCR gerou PDF branco."
            )

        # IMPORTANTE: marcamos como resultado para não deletar no finally
        result_pdf = ocr_output_pdf
        return {"page": page_num, "pdf": ocr_output_pdf, "success": True, "error": None}

    except Exception as e:
        logging.error(f"[HP-OCR] Erro na página {page_num}: {e}")
        return {"page": page_num, "pdf": None, "success": False, "error": str(e)}

    finally:
        # Limpa intermediários, MAS NUNCA deleta o resultado se foi bem-sucedido
        # (result_pdf só é definido se retorna sucesso)

        # 1. Limpar intermediários do Tesseract (.txt, .hocr, .log, etc)
        for ext in [".txt", ".hocr", ".log"]:
            aux_file = ocr_output_base + ext
            if os.path.isfile(aux_file):
                try:
                    os.remove(aux_file)
                except OSError:
                    pass

        # 2. Limpar arquivos PDF e PNG (exceto o resultado)
        for temp_file in [page_pdf, img_path]:
            # Pula o arquivo que contém o resultado
            if temp_file == result_pdf:
                continue
            if temp_file and os.path.isfile(temp_file):
                try:
                    os.remove(temp_file)
                except OSError:
                    pass


def _compress_page_extra(
    page_pdf: str,
    gs_cfg: dict,
    work_dir: str = None,
    light_mode: bool = False,
    aggressive_selected: bool = False,
    force_compress: bool = False,
) -> str:
    """
    Comprime uma página individual para ≤ PAGE_SIZE_LIMIT (500 KB) ou
    PAGE_SIZE_LIMIT_SPLIT (1.5 MB) no modo light_mode.

    Estratégia raster (sem Ghostscript):
        pdftoppm renderiza a página como PNG
        → PIL recomprime como JPEG
        → pikepdf monta novo PDF de página única

    Age em páginas acima do limite ou quando force_compress=True.
    Tenta passadas progressivas
    (_DPI_PASSES ou _DPI_PASSES_SPLIT) até atingir o limite ou esgotar as opções.
    Sobrescreve o arquivo original se a versão comprimida for menor.

    Nota: esta função opera sobre fragmentos de página única pós-OCR.
    A camada de texto do OCRmyPDF é incorporada como texto invisível sobre
    a imagem JPEG — o conteúdo visual nunca é perdido.

    Args:
        page_pdf:   Caminho do PDF de página única (tipicamente no RAM Disk).
        gs_cfg:     Config de compressão GS (mantido para compatibilidade de
                    assinatura, não usado nesta implementação).
        work_dir:   Diretório temporário (padrão: mesmo dir de page_pdf).
        light_mode: Se True, usa compressão mais leve (modo dividir).
        aggressive_selected: Se True, aplica preset equivalente ao modo
                72 DPI para páginas marcadas no seletor de compressão extra.
        force_compress: Se True, força tentativa de recompressão mesmo abaixo
                    do limite de tamanho.
    """
    size_limit = PAGE_SIZE_LIMIT_SPLIT if light_mode else PAGE_SIZE_LIMIT
    dpi_passes = list(_DPI_PASSES_SPLIT if light_mode else _DPI_PASSES)

    # Para páginas marcadas manualmente pelo usuário, usa o preset do modo
    # 72 DPI para manter o mesmo comportamento do botão global correspondente.
    if aggressive_selected:
        dpi_passes = [(_EXTRA_SELECTED_DPI_72, _EXTRA_SELECTED_QUALITY_72)]

        logging.info(
            "[HP-OCR] Página marcada para compressão extra usando preset 72 DPI "
            f"(dpi={_EXTRA_SELECTED_DPI_72}, q={_EXTRA_SELECTED_QUALITY_72})"
        )

    current_size = os.path.getsize(page_pdf)
    if current_size <= size_limit and not force_compress:
        return page_pdf  # já dentro do limite, não faz nada

    tmp_dir   = Path(work_dir or os.path.dirname(page_pdf))
    page_name = Path(page_pdf).name
    best_path: str | None = None
    best_size = current_size
    last_valid_path: str | None = None  # ← Rastreia última versão não-branca boa

    for pass_idx, (dpi, quality) in enumerate(dpi_passes, 1):
        prefix = str(tmp_dir / f"_raster_{pass_idx}_{page_name}")

        # Rasteriza via pdftoppm (sempre página 1 — fragmento é página única)
        result = subprocess.run(
            ["pdftoppm", "-f", "1", "-l", "1",
             "-r", str(dpi), "-png", page_pdf, prefix],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            logging.warning(
                f"[HP-OCR] pdftoppm falhou (passada {pass_idx}): "
                f"{result.stderr[:120]}"
            )
            continue

        png_files = sorted(tmp_dir.glob(f"_raster_{pass_idx}_{page_name}-*.png"))
        if not png_files:
            logging.warning(f"[HP-OCR] pdftoppm não gerou PNG (passada {pass_idx})")
            continue

        try:
            img = Image.open(png_files[0]).convert("RGB")
            w_px, h_px = img.size
            w_pt = round(w_px * 72 / dpi, 2)
            h_pt = round(h_px * 72 / dpi, 2)

            # Recomprime como JPEG em memória
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            jpg_bytes = buf.getvalue()

            # Validar JPEG antes de montar em PDF
            if not _validate_jpeg_bytes(jpg_bytes):
                logging.warning(
                    f"[HP-OCR] JPEG inválido gerado na passada {pass_idx}. "
                    f"Descartando passada."
                )
                continue

            # Sanity check: rejeita JPEG suspeito (< 1 KB = imagem corrompida/vazia)
            if len(jpg_bytes) < 1024:
                logging.warning(
                    f"[HP-OCR] JPEG gerado na passada {pass_idx} é suspeito "
                    f"({len(jpg_bytes)} bytes). Descartando passada."
                )
                continue

            # Monta PDF de página única com pikepdf
            pdf_out   = pikepdf.Pdf.new()
            image_obj = pdf_out.make_stream(
                jpg_bytes,
                **{
                    "/Type":             pikepdf.Name("/XObject"),
                    "/Subtype":          pikepdf.Name("/Image"),
                    "/Width":            w_px,
                    "/Height":           h_px,
                    "/ColorSpace":       pikepdf.Name("/DeviceRGB"),
                    "/BitsPerComponent": 8,
                    "/Filter":           pikepdf.Name("/DCTDecode"),
                }
            )
            img_ref     = pdf_out.make_indirect(image_obj)
            content_obj = pdf_out.make_indirect(
                pdf_out.make_stream(
                    f"q {w_pt} 0 0 {h_pt} 0 0 cm /Im0 Do Q".encode()
                )
            )
            page_dict = pikepdf.Dictionary(
                Type=pikepdf.Name("/Page"),
                MediaBox=pikepdf.Array(
                    [0, 0, Decimal(str(w_pt)), Decimal(str(h_pt))]
                ),
                Resources=pikepdf.Dictionary(
                    XObject=pikepdf.Dictionary(Im0=img_ref)
                ),
                Contents=content_obj,
            )
            pdf_out.pages.append(pikepdf.Page(pdf_out.make_indirect(page_dict)))

            compressed_path = str(tmp_dir / f"_comp_{pass_idx}_{page_name}")
            pdf_out.save(compressed_path)
            new_size = os.path.getsize(compressed_path)

            # Validar PDF comprimido antes de usar
            if not _validate_pdf_integrity(compressed_path):
                logging.warning(
                    f"[HP-OCR] PDF comprimido inválido na passada {pass_idx}. "
                    f"Descartando."
                )
                os.remove(compressed_path)
                continue

            # ⚠️ CRÍTICO: Validar se NÃO ficou branca após compressão
            if _detect_blank_pdf(compressed_path):
                logging.warning(
                    f"[HP-OCR] PDF comprimido ficou BRANCA na passada {pass_idx}. "
                    f"VOLTANDO para última versão válida (passada anterior)."
                )
                os.remove(compressed_path)
                # ⚠️ Se uma passada fica branca, usa a última válida que funcionou
                # e para aqui (não tenta passadas mais agressivas)
                if last_valid_path:
                    logging.info(
                        f"[HP-OCR] Usando passada anterior (não-branca): "
                        f"{best_size // 1024}KB"
                    )
                    break
                else:
                    # Se nenhuma passada anterior funcionou, continua tentando outras
                    continue

            # Sanity check: rejeita PDF comprimido suspeito (< 1 KB)
            if new_size < 1024:
                logging.warning(
                    f"[HP-OCR] PDF comprimido na passada {pass_idx} é suspeito "
                    f"({new_size} bytes). Descartando."
                )
                os.remove(compressed_path)
                continue

            logging.info(
                f"[HP-OCR] Passada {pass_idx} ({dpi}dpi q{quality}): "
                f"{current_size // 1024}KB → {new_size // 1024}KB"
            )

            # ⚠️ Registra esta passada como válida (não-branca)
            if new_size < best_size:
                if best_path and os.path.exists(best_path):
                    os.remove(best_path)
                best_path = compressed_path
                best_size = new_size
                last_valid_path = compressed_path  # ← Marca como versão válida
                logging.debug(
                    f"[HP-OCR] Nova melhor versão encontrada (não-branca): "
                    f"passada {pass_idx}, tamanho {best_size // 1024}KB"
                )
            else:
                os.remove(compressed_path)

        except Exception as e:
            logging.warning(f"[HP-OCR] Falha na passada {pass_idx}: {e}")

        finally:
            for f in png_files:
                f.unlink(missing_ok=True)

        if best_size <= size_limit and (not force_compress or aggressive_selected):
            break  # limite atingido — para aqui

    # Aplica o melhor resultado (somente se houve melhora E arquivo é válido)
    if best_path and os.path.exists(best_path):
        final_size = os.path.getsize(best_path)

        # Validar integridade do PDF melhor antes de sobrescrever
        if not _validate_pdf_integrity(best_path):
            logging.error(
                f"[HP-OCR] Arquivo comprimido inválido/corrompido. "
                f"Mantendo original para evitar página branca."
            )
            os.remove(best_path)
            return page_pdf

        if final_size >= 1024:  # sanity check final antes de sobrescrever
            try:
                os.replace(best_path, page_pdf)
                limit_kb = size_limit // 1024
                logging.info(
                    f"[HP-OCR] Compressão final: {current_size // 1024}KB → "
                    f"{final_size // 1024}KB "
                    f"({'OK ≤' + str(limit_kb) + 'KB' if final_size <= size_limit else 'AINDA >' + str(limit_kb) + 'KB'})"
                )
            except Exception as replace_err:
                logging.error(f"[HP-OCR] Falha ao aplicar compressão: {replace_err}")
                # Mantém original se sobrescrita falhar
                os.remove(best_path)
        else:
            logging.error(
                f"[HP-OCR] Arquivo comprimido final suspeito ({final_size}B). "
                f"Mantendo original para evitar página branca."
            )
            os.remove(best_path)
    else:
        logging.warning(
            f"[HP-OCR] Nenhuma passada reduziu o arquivo. "
            f"Mantendo original ({current_size // 1024}KB)"
        )

    return page_pdf


def _merge_pdfs_ghostscript(
    pdf_fragments: list,
    output_path: str,
    gs_cfg: dict,
    progress_callback=None,
) -> None:
    """
    Merge final de todos os fragmentos PDF usando Ghostscript com
    compressão agressiva. As imagens são re-comprimidas para reduzir
    drasticamente o tamanho.
    """
    gs = _locate_gs()
    cmd = [
        gs,
        "-dBATCH",
        "-dNOPAUSE",
        "-dQUIET",
        "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.5",
        f"-dPDFSETTINGS={gs_cfg['pdf_settings']}",
        f"-dNumRenderingThreads={GS_RENDERING_THREADS}",
        f"-dBufferSpace={GS_BUFFER_SPACE}",
        "-dAutoRotatePages=/None",
        "-sPAPERSIZE=a4",
        "-dFIXEDMEDIA",
        "-dPDFFitPage",
        "-dEmbedAllFonts=true",
        "-dSubsetFonts=true",
        "-dDownsampleColorImages=true",
        "-dColorImageDownsampleType=/Bicubic",
        f"-dColorImageResolution={gs_cfg['dpi']}",
        "-dDownsampleGrayImages=true",
        "-dGrayImageDownsampleType=/Bicubic",
        f"-dGrayImageResolution={gs_cfg['dpi']}",
        "-dDownsampleMonoImages=true",
        "-dMonoImageDownsampleType=/Bicubic",
        f"-dMonoImageResolution={gs_cfg['dpi']}",
        "-dAutoFilterColorImages=false",
        "-dColorImageFilter=/DCTEncode",
        "-dAutoFilterGrayImages=false",
        "-dGrayImageFilter=/DCTEncode",
        f"-sOutputFile={output_path}",
        "-c",
        (
            f"<< /ColorACSImageDict << /QFactor {gs_cfg.get('qfactor', 0.76)} "
            f"/Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> "
            f"/GrayACSImageDict << /QFactor {gs_cfg.get('qfactor', 0.76)} "
            f"/Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> "
            f">> setdistillerparams"
        ),
        "-f",
    ] + pdf_fragments

    logging.info(
        f"[HP-OCR] Ghostscript merge+compress: "
        f"{len(pdf_fragments)} fragmentos → {output_path}"
    )
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )

    started = time.time()
    timeout_s = 600

    while proc.poll() is None:
        elapsed = int(time.time() - started)
        if progress_callback:
            try:
                progress_callback(elapsed)
            except Exception:
                pass

        if elapsed > timeout_s:
            proc.kill()
            _, stderr = proc.communicate()
            raise RuntimeError(
                "Ghostscript merge excedeu timeout de 600s. "
                f"stderr: {(stderr or '').strip()[:300]}"
            )
        time.sleep(1)

    _, stderr = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"Ghostscript merge falhou (exit {proc.returncode}): "
            f"{(stderr or '').strip()}"
        )

    # Validar se GS gerou PDF válido (não vazio/corrompido)
    if not _validate_pdf_integrity(output_path):
        raise RuntimeError(
            f"Ghostscript gerou PDF inválido ou vazio: {output_path}"
        )


def _extra_compression(
    input_path: str,
    output_path: str,
    gs_cfg: dict,
    progress_callback=None,
) -> None:
    """
    Passo extra de compressão agressiva via Ghostscript.
    Usado quando o merge ainda produz um arquivo grande.
    """
    gs = _locate_gs()
    cmd = [
        gs,
        "-dBATCH", "-dNOPAUSE", "-dQUIET", "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.5",
        "-dPDFSETTINGS=/screen",
        f"-dNumRenderingThreads={GS_RENDERING_THREADS}",
        "-dEmbedAllFonts=true",
        "-dSubsetFonts=true",
        "-dDownsampleColorImages=true",
        "-dColorImageDownsampleType=/Bicubic",
        f"-dColorImageResolution={gs_cfg['extra_dpi']}",
        "-dDownsampleGrayImages=true",
        "-dGrayImageDownsampleType=/Bicubic",
        f"-dGrayImageResolution={gs_cfg['extra_dpi']}",
        "-dDownsampleMonoImages=true",
        f"-dMonoImageResolution={gs_cfg['extra_dpi']}",
        "-dAutoFilterColorImages=false",
        "-dColorImageFilter=/DCTEncode",
        "-dAutoFilterGrayImages=false",
        "-dGrayImageFilter=/DCTEncode",
        f"-sOutputFile={output_path}",
        "-c",
        (
            f"<< /ColorACSImageDict << /QFactor {gs_cfg.get('extra_qfactor', 0.50)} "
            f"/Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> "
            f"/GrayACSImageDict << /QFactor {gs_cfg.get('extra_qfactor', 0.50)} "
            f"/Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> "
            f">> setdistillerparams"
        ),
        "-f",
        input_path,
    ]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        started = time.time()
        timeout_s = 300

        while proc.poll() is None:
            elapsed = int(time.time() - started)
            if progress_callback:
                try:
                    progress_callback(elapsed)
                except Exception:
                    pass

            if elapsed > timeout_s:
                proc.kill()
                _, stderr = proc.communicate()
                logging.warning(
                    "[HP-OCR] Compressão extra excedeu timeout de 300s: "
                    f"{(stderr or '').strip()[:200]}"
                )
                return
            time.sleep(1)

        _, stderr = proc.communicate()
        if proc.returncode != 0:
            logging.warning(
                f"[HP-OCR] Compressão extra falhou: {(stderr or '').strip()[:200]}"
            )
    except Exception as e:
        logging.warning(f"[HP-OCR] Compressão extra exceção: {e}")


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

def process_pdf_high_performance(
    input_path: str,
    callback=None,
    compression_level: int | None = None,
    extra_compress_pages: list[int] | None = None,
    skip_extra_compression: bool = False,
) -> str:
    """
    Processa um PDF com OCR de alta performance usando paralelização massiva.

    Pipeline:
        1. Conta páginas do PDF de entrada
        2. Cada worker (ProcessPoolExecutor) extrai sua página via pikepdf,
           faz triagem de texto e aplica OCR via fitz→PNG→tesseract se necessário
        3. Comprime páginas acima de 500 KB via rasterização (pdftoppm + PIL)
        4. Faz merge final (ghostscript) com flags otimizadas para Xeon
        5. Passo extra de compressão GS se arquivo final > threshold

    Por que fitz→PNG→Tesseract (não OCRmyPDF):
        OCRmyPDF pode gerar PDFs vazios/brancos em cenários de alta carga.
        O pipeline manual com fragmentos isolados é mais previsível e comprovadamente
        seguro — evita concorrência no arquivo original e nunca perde conteúdo.

    skip_extra_compression: quando True (ex.: modo OCR + DIVIDIR), omite o passo
        de compressão extra pós-merge — o chamador será responsável por dividir o
        arquivo em volumes, preservando qualidade.

    Args:
        input_path:             Caminho absoluto do PDF de entrada.
        callback:               Função opcional callback(current_page, total_pages).
        compression_level:      Nível de compressão GS (1–7 ou DPI direto).
        extra_compress_pages:   Lista de páginas para compressão extra pelo usuário.
        skip_extra_compression: Se True, omite o passo GS extra pós-merge.

    Returns:
        Caminho absoluto do PDF final otimizado (no diretório do input).

    Raises:
        FileNotFoundError: Se o arquivo de entrada não existir.
        RuntimeError:      Se o processamento falhar criticamente.
    """
    input_path = os.path.abspath(input_path)
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")

    session_id   = uuid.uuid4().hex[:16]
    ramdisk_base = _get_ramdisk_dir()
    work_dir     = os.path.join(ramdisk_base, f"session_{session_id}")
    os.makedirs(work_dir, exist_ok=True)

    # Validar espaço disponível em RAM Disk
    available_gb = _get_ramdisk_space(work_dir)
    if available_gb < MIN_RAMDISK_SPACE_MB / 1024:  # Converter MB para GB
        if available_gb < 0.1:  # Muito crítico (< 100 MB)
            raise RuntimeError(
                f"RAM Disk quase cheio: apenas {available_gb:.2f} GB disponível. "
                f"Mínimo recomendado: {MIN_RAMDISK_SPACE_MB / 1024:.2f} GB. "
                f"Limpe arquivos antigos em {ramdisk_base}"
            )
        logging.warning(
            f"[HP-OCR] RAM Disk com espaço limitado: "
            f"{available_gb:.2f} GB disponível (recomendado: {MIN_RAMDISK_SPACE_MB / 1024:.2f} GB). "
            f"Performance pode ser afetada."
        )

    input_stem  = Path(input_path).stem
    input_dir   = os.path.dirname(input_path)
    output_path = os.path.join(input_dir, f"{input_stem}_hp_ocr.pdf")

    logging.info(
        f"[HP-OCR] Início | input={input_path} | workers={MAX_WORKERS} | "
        f"ramdisk={work_dir}"
    )

    gs_cfg          = _resolve_gs_compression(compression_level)
    extra_pages_set = set(extra_compress_pages) if extra_compress_pages else set()
    logging.info(
        f"[HP-OCR] Compressão GS | nível={compression_level} | "
        f"dpi={gs_cfg['dpi']} | perfil={gs_cfg['pdf_settings']} | "
        f"páginas_extra={sorted(extra_pages_set) if extra_pages_set else 'nenhuma'}"
    )

    try:
        # ── 1. Contagem de páginas ──────────────────────────────────────
        total_pages = _get_page_count(input_path)
        logging.info(f"[HP-OCR] Total de páginas: {total_pages}")
        _emit_progress(
            callback,
            0,
            max(1, total_pages),
            stage="prepare",
            label="Preparando OCR",
            detail=f"{total_pages} página(s) detectadas",
        )

        if total_pages == 0:
            shutil.copy2(input_path, output_path)
            logging.warning("[HP-OCR] PDF com 0 páginas. Retornando cópia.")
            return output_path

        # ── 2. Processamento paralelo (pikepdf + OCRmyPDF) ─────────────
        tasks = [
            (input_path, page_num, work_dir)
            for page_num in range(1, total_pages + 1)
        ]

        results      = [None] * total_pages
        completed    = 0
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
                        "page": page_num, "pdf": None,
                        "success": False, "error": str(e),
                    }
                    logging.error(
                        f"[HP-OCR] Exceção no worker da página {page_num}: {e}"
                    )
                finally:
                    completed += 1
                    _emit_progress(
                        callback,
                        completed,
                        total_pages,
                        stage="ocr_pages",
                        label="OCR por página",
                        detail=f"{completed}/{total_pages} páginas processadas",
                    )

        # ── 3. Coleta dos fragmentos bem-sucedidos (na ordem) ───────────
        pdf_fragments      = []
        page_statuses      = []  # (page_num, status) para diagnóstico

        for r in results:
            if r and r["success"] and r["pdf"] and os.path.isfile(r["pdf"]):
                pdf_fragments.append(r["pdf"])
                page_statuses.append((r["page"], "OK"))
            elif r:
                page_num = r["page"]
                failed_pages.append(page_num)
                page_statuses.append((page_num, "FALHA"))

        if not pdf_fragments:
            raise RuntimeError(
                f"Nenhuma página foi processada com sucesso. "
                f"Falhas: {len(failed_pages)}/{total_pages}"
            )

        # Log de diagnóstico completo
        statuses_str = ", ".join(f"pág{p}:{s}" for p, s in sorted(page_statuses))
        logging.info(f"[HP-OCR] Status de páginas: {statuses_str}")

        if failed_pages:
            raise RuntimeError(
                f"[HP-OCR] {len(failed_pages)} página(s) falharam no OCR: {failed_pages}"
            )

        # ── 3b. Compressão por página ───────────────────────────────────
        #
        # Aplica em DUAS situações (união):
        #   a) Páginas selecionadas explicitamente pelo usuário (extra_pages_set)
        #   b) TODAS as páginas — qualquer fragmento acima do limite
        #
        # No modo dividir (skip_extra_compression=True), usa compressão mais leve
        # (limite 1.5 MB) pois a divisão em volumes vai distribuir o tamanho.
        light_mode = skip_extra_compression
        page_limit = PAGE_SIZE_LIMIT_SPLIT if light_mode else PAGE_SIZE_LIMIT
        logging.info(
            f"[HP-OCR] Compressão por página "
            f"(limite {page_limit // 1024} KB, light_mode={light_mode})..."
        )
        pages_comprimidas = 0
        pages_avaliadas = 0
        total_avaliacoes = len(results)

        for r in results:
            if not (r and r["success"] and r["pdf"] and os.path.isfile(r["pdf"])):
                pages_avaliadas += 1
                _emit_progress(
                    callback,
                    pages_avaliadas,
                    max(1, total_avaliacoes),
                    stage="page_compress",
                    label="Compressão por página",
                    detail=(
                        f"{pages_avaliadas}/{total_avaliacoes} páginas avaliadas "
                        f"({pages_comprimidas} comprimidas)"
                    ),
                )
                continue

            frag           = r["pdf"]
            frag_size      = os.path.getsize(frag)
            selected_extra = r["page"] in extra_pages_set
            needs_compress = (
                frag_size > page_limit
                or selected_extra
            )

            if not needs_compress:
                pages_avaliadas += 1
                _emit_progress(
                    callback,
                    pages_avaliadas,
                    max(1, total_avaliacoes),
                    stage="page_compress",
                    label="Compressão por página",
                    detail=(
                        f"{pages_avaliadas}/{total_avaliacoes} páginas avaliadas "
                        f"({pages_comprimidas} comprimidas)"
                    ),
                )
                continue

            orig_kb = frag_size // 1024
            _compress_page_extra(
                frag,
                gs_cfg,
                work_dir=work_dir,
                light_mode=light_mode,
                aggressive_selected=selected_extra,
                force_compress=selected_extra,
            )
            new_kb = os.path.getsize(frag) // 1024
            pages_comprimidas += 1
            pages_avaliadas += 1
            logging.info(f"[HP-OCR] Pág. {r['page']}: {orig_kb}KB → {new_kb}KB")
            _emit_progress(
                callback,
                pages_avaliadas,
                max(1, total_avaliacoes),
                stage="page_compress",
                label="Compressão por página",
                detail=(
                    f"{pages_avaliadas}/{total_avaliacoes} páginas avaliadas "
                    f"({pages_comprimidas} comprimidas)"
                ),
            )

        logging.info(
            f"[HP-OCR] Compressão de páginas concluída: "
            f"{pages_comprimidas} de {len(pdf_fragments)} fragmentos processados"
        )

        # ── 4. Merge final com Ghostscript ─────────────────────────────
        merged_tmp = os.path.join(work_dir, "merged_final.pdf")
        merge_eta = max(12, min(180, int(total_pages * 0.7)))
        _emit_progress(
            callback,
            0,
            merge_eta,
            stage="merge",
            label="Mesclando páginas",
            detail="Unificando fragmentos do OCR",
        )
        _merge_pdfs_ghostscript(
            pdf_fragments,
            merged_tmp,
            gs_cfg,
            progress_callback=lambda elapsed: _emit_progress(
                callback,
                min(elapsed, merge_eta),
                merge_eta,
                stage="merge",
                label="Mesclando páginas",
                detail=f"Mesclagem em andamento ({elapsed}s)",
            ),
        )
        _emit_progress(
            callback,
            merge_eta,
            merge_eta,
            stage="merge",
            label="Mesclando páginas",
            detail="Mesclagem concluída",
        )

        # Validar PDF após merge (função _merge_pdfs_ghostscript já valida, mas dupla-check)
        if not _validate_pdf_integrity(merged_tmp):
            raise RuntimeError("Merge final gerou PDF inválido ou vazio.")

        if not os.path.isfile(merged_tmp):
            raise RuntimeError("Ghostscript não gerou o arquivo de merge final.")

        merged_size_mb = os.path.getsize(merged_tmp) / (1024 * 1024)
        logging.info(f"[HP-OCR] Merge concluído: {merged_size_mb:.2f} MB")

        # ── 5. Passo extra de compressão GS se necessário ───────────────
        # Pulado quando skip_extra_compression=True (modo OCR + DIVIDIR).
        if not skip_extra_compression and merged_size_mb > EXTRA_COMPRESS_THRESHOLD_MB:
            logging.info(
                f"[HP-OCR] Arquivo > {EXTRA_COMPRESS_THRESHOLD_MB}MB. "
                f"Aplicando compressão extra GS..."
            )
            compressed_tmp = os.path.join(work_dir, "compressed_final.pdf")
            extra_eta = max(8, min(120, int(total_pages * 0.4)))
            _emit_progress(
                callback,
                0,
                extra_eta,
                stage="extra_compress",
                label="Compressão final",
                detail="Aplicando compressão extra",
            )
            _extra_compression(
                merged_tmp,
                compressed_tmp,
                gs_cfg,
                progress_callback=lambda elapsed: _emit_progress(
                    callback,
                    min(elapsed, extra_eta),
                    extra_eta,
                    stage="extra_compress",
                    label="Compressão final",
                    detail=f"Compressão em andamento ({elapsed}s)",
                ),
            )
            _emit_progress(
                callback,
                extra_eta,
                extra_eta,
                stage="extra_compress",
                label="Compressão final",
                detail="Compressão extra concluída",
            )
            if os.path.isfile(compressed_tmp):
                comp_size_mb = os.path.getsize(compressed_tmp) / (1024 * 1024)
                logging.info(
                    f"[HP-OCR] Compressão extra GS: "
                    f"{merged_size_mb:.2f} MB → {comp_size_mb:.2f} MB"
                )
                # Validar se não ficou branca após compressão extra
                if _detect_blank_pdf(compressed_tmp):
                    logging.warning(
                        f"[HP-OCR] Compressão extra gerou PDF BRANCA. "
                        f"Mantendo versão anterior."
                    )
                    os.remove(compressed_tmp)
                elif comp_size_mb < merged_size_mb:
                    os.replace(compressed_tmp, merged_tmp)
                else:
                    os.remove(compressed_tmp)

        # Move do RAM Disk para o destino final
        _emit_progress(
            callback,
            0,
            1,
            stage="finalize",
            label="Finalizando arquivo",
            detail="Movendo e validando PDF final",
        )
        shutil.move(merged_tmp, output_path)

        # Validar PDF final antes de retornar
        if not _validate_pdf_integrity(output_path):
            raise RuntimeError(f"PDF final inválido ou vazio: {output_path}")

        final_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logging.info(
            f"[HP-OCR] ✅ Concluído com SUCESSO | {total_pages} páginas | "
            f"{len(failed_pages)} falhas | "
            f"output={output_path} ({final_size_mb:.2f} MB)"
        )

        _emit_progress(
            callback,
            1,
            1,
            stage="done",
            label="OCR concluído",
            detail=f"{total_pages} páginas processadas",
        )

        return output_path

    except Exception:
        logging.exception("[HP-OCR] Falha crítica no processamento")
        raise

    finally:
        _cleanup_work_dir(work_dir)
