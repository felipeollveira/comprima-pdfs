"""
Motor de compressão GS de alta performance para PDFs.

Usa a mesma estratégia do HP-OCR:
- ProcessPoolExecutor com workers otimizados para Xeon E5-2620 v4
- RAM Disk para I/O zero-latência
- Ghostscript com -dNumRenderingThreads e -dBufferSpace para merge
- subprocess puro (sem dependência de fitz nos workers)
"""

import subprocess
import os
import fitz
import logging
import uuid
import shutil
import concurrent.futures
from engines.locate_gs import localizar_gs
from engines.force_ocr import ocr, LIMITE_CHARS_OCR
from engines.ramdisk import temp_dir

# ── Configurações (mesmas do HP-OCR) ────────────────────────────────────────
MAX_WORKERS = 10               # Aproveita threads do Xeon, reserva para OS/Flask
GS_RENDERING_THREADS = 12
GS_BUFFER_SPACE = 1_000_000_000  # 1 GB de buffer para merge final
PERFIS_GS = {1: '/prepress', 2: '/printer', 3: '/ebook', 4: '/screen', 5: '/screen'}
# ─────────────────────────────────────────────────────────────────────────────


def _gs_extract_page(gs_exe, input_path, page_num, output_path):
    """Extrai uma única página via Ghostscript (subprocess, sem fitz)."""
    cmd = [
        gs_exe,
        "-dBATCH", "-dNOPAUSE", "-dQUIET", "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        f"-dFirstPage={page_num}",
        f"-dLastPage={page_num}",
        f"-sOutputFile={output_path}",
        input_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            f"GS extração página {page_num} falhou: {result.stderr.strip()[:200]}"
        )


def _gs_compress_page(gs_exe, input_path, output_path, perfil):
    """Comprime uma única página via Ghostscript com perfil DPI."""
    cmd = [
        gs_exe,
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        f"-dPDFSETTINGS={perfil}",
        "-dNOPAUSE", "-dQUIET", "-dBATCH", "-dSAFER",
        "-dAlwaysEmbed=false",
        "-dEmbedAllFonts=false",
        f"-sOutputFile={output_path}",
        input_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            f"GS compressão falhou: {result.stderr.strip()[:200]}"
        )


def _worker_gs_page(args):
    """
    Worker de alta performance executado em processo separado.
    Pipeline por página: extrair → triagem OCR → comprimir → retornar caminho.
    Aplica OCR se a página tem imagem dominante (>30% da área) e pouco texto (<50 palavras).
    Todo I/O no RAM Disk.
    """
    page_idx, input_path, nivel, work_dir = args
    page_num = page_idx + 1  # GS usa 1-indexed
    uid = uuid.uuid4().hex[:12]
    perfil = PERFIS_GS.get(int(nivel), '/ebook')

    t_raw = os.path.join(work_dir, f"raw_{page_num:05d}_{uid}.pdf")
    t_ocr = os.path.join(work_dir, f"ocr_{page_num:05d}_{uid}.pdf")
    t_out = os.path.join(work_dir, f"out_{page_num:05d}_{uid}.pdf")

    try:
        gs_exe = localizar_gs()

        # 1. Extrair página via GS
        _gs_extract_page(gs_exe, input_path, page_num, t_raw)

        if not os.path.isfile(t_raw):
            raise FileNotFoundError(f"Extração não gerou arquivo: {t_raw}")

        # 2. Triagem inteligente: detecta imagens e texto (mesmo critério do force_ocr)
        precisa_ocr = False
        tem_imagem = False
        palavra_count = 0
        char_count = 0
        try:
            with fitz.open(t_raw) as check_doc:
                page = check_doc[0]
                texto_puro = page.get_text().strip()
                palavra_count = len(texto_puro.split())
                char_count = len(texto_puro)

                # Detecta imagens na página
                imagens = page.get_images(full=True)
                tem_imagem = len(imagens) > 0

                # Calcula área das imagens vs área da página
                page_area = page.rect.width * page.rect.height
                imagem_area_total = 0
                for img in imagens:
                    try:
                        xref = img[0]
                        img_rects = page.get_image_rects(xref)
                        for rect in img_rects:
                            imagem_area_total += rect.width * rect.height
                    except:
                        pass

                # Imagem dominante: ocupa mais de 30% da página
                imagem_dominante = imagem_area_total > (page_area * 0.3) if page_area > 0 else False

                # Precisa OCR se: tem imagem dominante E pouco texto (< 200 chars OU < 50 palavras)
                precisa_ocr = tem_imagem and imagem_dominante and (char_count < LIMITE_CHARS_OCR or palavra_count < 50)
        except Exception as triage_err:
            logging.warning(f"[HP-GS] Triagem falhou na página {page_num}: {triage_err}")

        # 3. OCR se necessário
        if precisa_ocr:
            logging.info(f"[HP-GS] Página {page_num}: imagem dominante, {palavra_count} palavras, {char_count} chars → OCR ativado")
            try:
                ocr(t_raw, t_ocr, pages=None)
            except Exception as ocr_err:
                logging.warning(f"[HP-GS] OCR falhou na página {page_num}, usando raw: {ocr_err}")
                shutil.copy(t_raw, t_ocr)
        else:
            if tem_imagem:
                logging.info(f"[HP-GS] Página {page_num}: texto suficiente ({palavra_count} palavras), OCR pulado")
            else:
                logging.info(f"[HP-GS] Página {page_num}: texto digital, OCR pulado")
            shutil.copy(t_raw, t_ocr)

        # 4. Comprimir página via GS com perfil DPI
        source_for_compress = t_ocr if os.path.isfile(t_ocr) else t_raw
        try:
            _gs_compress_page(gs_exe, source_for_compress, t_out, perfil)
        except Exception as gs_err:
            logging.warning(f"[HP-GS] Compressão falhou na página {page_num}, usando fonte: {gs_err}")
            shutil.copy(source_for_compress, t_out)

        if not os.path.isfile(t_out):
            raise FileNotFoundError(f"Compressão não gerou arquivo: {t_out}")

        return {
            "page": page_idx,
            "pdf": t_out,
            "success": True,
            "error": None,
        }

    except Exception as e:
        logging.error(f"[HP-GS] Erro fatal na página {page_num}: {e}")
        # Fallback: retorna o que existir
        for fallback in [t_ocr, t_raw]:
            if os.path.isfile(fallback):
                return {
                    "page": page_idx,
                    "pdf": fallback,
                    "success": True,
                    "error": f"fallback: {e}",
                }
        return {
            "page": page_idx,
            "pdf": None,
            "success": False,
            "error": str(e),
        }

    finally:
        # Limpa intermediários (out será limpo após merge)
        for f in [t_raw, t_ocr]:
            if os.path.isfile(f) and f != t_out:
                try:
                    os.remove(f)
                except OSError:
                    pass


def _gs_merge_hp(pdf_fragments, output_path, perfil="/ebook"):
    """
    Merge final de todos os fragmentos via Ghostscript com flags Xeon.
    Mesma estratégia do HP-OCR.
    """
    gs_exe = localizar_gs()
    cmd = [
        gs_exe,
        "-dBATCH", "-dNOPAUSE", "-dQUIET", "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        f"-dPDFSETTINGS={perfil}",
        f"-dNumRenderingThreads={GS_RENDERING_THREADS}",
        f"-dBufferSpace={GS_BUFFER_SPACE}",
        "-dAutoRotatePages=/None",
        "-dColorImageDownsampleType=/Bicubic",
        "-dGrayImageDownsampleType=/Bicubic",
        f"-sOutputFile={output_path}",
    ] + pdf_fragments

    logging.info(f"[HP-GS] Merge: {len(pdf_fragments)} fragmentos → {output_path}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(
            f"GS merge falhou (exit {result.returncode}): {result.stderr.strip()[:300]}"
        )


def processar_pdf_custom(input_path, output_path, config_map, callback=None, check_cancelled=None):
    """
    Motor GS de alta performance com ProcessPoolExecutor.
    Mesma estratégia do HP-OCR: paralelização massiva + RAM Disk + GS otimizado.
    """
    # Contagem de páginas (usa fitz apenas aqui, no processo principal)
    doc = fitz.open(input_path)
    total = len(doc)
    doc.close()

    if total == 0:
        shutil.copy2(input_path, output_path)
        return

    # Diretório de trabalho isolado no RAM Disk
    session_id = uuid.uuid4().hex[:16]
    base_dir = temp_dir()
    work_dir = os.path.join(base_dir, f"gs_session_{session_id}")
    os.makedirs(work_dir, exist_ok=True)

    logging.info(f"[HP-GS] Processando {total} páginas com {MAX_WORKERS} workers | ramdisk={work_dir}")

    try:
        # Preparar tarefas (page_idx, input_path, nível, work_dir)
        tarefas = [
            (i, input_path, config_map.get(str(i), 3), work_dir)
            for i in range(total)
        ]

        results = [None] * total
        completed = 0
        failed_pages = []

        # ProcessPoolExecutor para paralelismo real (não limitado pelo GIL)
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {
                executor.submit(_worker_gs_page, t): t[0]
                for t in tarefas
            }

            for future in concurrent.futures.as_completed(future_to_page):
                # Checar cancelamento
                if check_cancelled and check_cancelled():
                    executor.shutdown(wait=False, cancel_futures=True)
                    logging.info("[HP-GS] Processamento cancelado pelo usuário.")
                    return

                page_idx = future_to_page[future]
                try:
                    result = future.result(timeout=300)
                    results[page_idx] = result

                    if not result["success"]:
                        failed_pages.append(page_idx)
                        logging.warning(f"[HP-GS] Página {page_idx} falhou: {result['error']}")
                except Exception as e:
                    failed_pages.append(page_idx)
                    results[page_idx] = {
                        "page": page_idx,
                        "pdf": None,
                        "success": False,
                        "error": str(e),
                    }
                    logging.error(f"[HP-GS] Exceção no worker da página {page_idx}: {e}")
                finally:
                    completed += 1
                    if callback:
                        try:
                            callback(completed - 1, total)
                        except Exception:
                            pass

        # Coletar fragmentos bem-sucedidos (na ordem)
        pdf_fragments = []
        for r in results:
            if r and r.get("pdf") and os.path.isfile(r["pdf"]):
                pdf_fragments.append(r["pdf"])

        if not pdf_fragments:
            first_error = "desconhecido"
            for r in results:
                if r and not r.get("success") and r.get("error"):
                    first_error = r["error"]
                    break
            raise RuntimeError(
                f"Nenhuma página processada. Falhas: {len(failed_pages)}/{total}. "
                f"Primeiro erro: {first_error}"
            )

        if failed_pages:
            logging.warning(f"[HP-GS] {len(failed_pages)} página(s) falharam: {failed_pages}")

        # Merge final com GS otimizado (threads + buffer)
        # Determina perfil dominante para o merge
        niveis = [int(config_map.get(str(i), 3)) for i in range(total)]
        nivel_dominante = max(set(niveis), key=niveis.count)
        perfil_merge = PERFIS_GS.get(nivel_dominante, '/ebook')

        merged_tmp = os.path.join(work_dir, "merged_final.pdf")
        _gs_merge_hp(pdf_fragments, merged_tmp, perfil_merge)

        if not os.path.isfile(merged_tmp):
            raise RuntimeError("[HP-GS] Merge final não gerou arquivo.")

        # Move do RAM Disk para destino final
        shutil.move(merged_tmp, output_path)

        final_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        logging.info(
            f"[HP-GS] Concluído | {total} páginas | {len(failed_pages)} falhas | "
            f"{final_size_mb:.2f} MB | perfil={perfil_merge}"
        )

    except Exception:
        logging.exception("[HP-GS] Falha crítica no processamento")
        raise

    finally:
        # Limpeza garantida do RAM Disk
        if os.path.isdir(work_dir):
            try:
                shutil.rmtree(work_dir, ignore_errors=True)
                logging.info(f"[HP-GS] Diretório temporário removido: {work_dir}")
            except Exception as e:
                logging.warning(f"[HP-GS] Falha ao limpar {work_dir}: {e}")