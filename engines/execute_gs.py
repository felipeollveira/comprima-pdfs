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
import pikepdf
import logging
import uuid
import shutil
import concurrent.futures
import gc
import psutil
from engines.locate_gs import localizar_gs
from engines.force_ocr import ocr
from engines.ramdisk import temp_dir
from engines.constants import (
    MAX_WORKERS,
    GS_RENDERING_THREADS,
    GS_BUFFER_SPACE,
    EXTRA_COMPRESS_THRESHOLD_MB,
    LIMITE_CHARS_OCR,
    MIN_IMAGE_AREA_RATIO,
)

# ── Perfis de Compressão GS ─────────────────────────────────────────────────
PERFIS_GS     = {1: '/printer', 2: '/ebook', 3: '/screen', 4: '/screen', 5: '/screen'}
PERFIS_DPI    = {1: 200, 2: 150, 3: 80, 4: 50, 5: 40}
PERFIS_QFACTOR = {1: 0.90, 2: 0.85, 3: 0.76, 4: 0.65, 5: 0.50}
# ─────────────────────────────────────────────────────────────────────────────

# ── Limite por página e escada de recompressão ───────────────────────────────
LIMITE_PAGINA_KB = 500

# Cada item: (perfil_gs, dpi, qfactor) — do menos ao mais agressivo
_ESCADA_REDUCAO = [
    ("/screen", 72,  0.45),
    ("/screen", 60,  0.35),
    ("/screen", 50,  0.25),
    ("/screen", 40,  0.18),
    ("/screen", 30,  0.10),
]
# Preset equivalente ao modo 72 DPI selecionado no front.
_PERFIL_72_DPI = ("/screen", 72, 0.45)
# ─────────────────────────────────────────────────────────────────────────────


def _limpar_memoria_worker():
    try:
        gc.collect(generation=2)
        gc.collect(generation=1)
        gc.collect(generation=0)
    except Exception as e:
        logging.debug(f"Aviso ao limpar memória: {e}")


def _get_memoria_processo():
    try:
        processo = psutil.Process(os.getpid())
        return processo.memory_info().rss / (1024 * 1024)
    except Exception:
        return 0


def _gs_extract_page(gs_exe, input_path, page_num, output_path):
    """Extrai uma única página via Ghostscript (subprocess, sem fitz)."""
    cmd = [
        gs_exe,
        "-dBATCH", "-dNOPAUSE", "-dQUIET", "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.5",
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


def _gs_compress_page(gs_exe, input_path, output_path, perfil, dpi=142, qfactor=0.70):
    """Comprime uma única página via Ghostscript com perfil DPI e downsample explícito."""
    cmd = [
        gs_exe,
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.5",
        f"-dPDFSETTINGS={perfil}",
        "-dNOPAUSE", "-dQUIET", "-dBATCH", "-dSAFER",
        "-dEmbedAllFonts=true",
        "-dSubsetFonts=true",
        "-dDownsampleColorImages=true",
        "-dColorImageDownsampleType=/Bicubic",
        f"-dColorImageResolution={dpi}",
        "-dDownsampleGrayImages=true",
        "-dGrayImageDownsampleType=/Bicubic",
        f"-dGrayImageResolution={dpi}",
        "-dDownsampleMonoImages=true",
        "-dMonoImageDownsampleType=/Bicubic",
        f"-dMonoImageResolution={dpi}",
        "-dAutoFilterColorImages=false",
        "-dColorImageFilter=/DCTEncode",
        "-dAutoFilterGrayImages=false",
        "-dGrayImageFilter=/DCTEncode",
        f"-sOutputFile={output_path}",
        "-c",
        f"<< /ColorACSImageDict << /QFactor {qfactor} /Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> /GrayACSImageDict << /QFactor {qfactor} /Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> >> setdistillerparams",
        "-f",
        input_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            f"GS compressão falhou: {result.stderr.strip()[:200]}"
        )


def _recomprimir_ate_limite(
    gs_exe,
    t_out,
    page_num,
    uid,
    work_dir,
    aggressive_selected=False,
    force_compress=False,
):
    """
    Recomprime `t_out` em loop com configurações progressivamente mais agressivas
    até que o arquivo fique ≤ LIMITE_PAGINA_KB ou todas as tentativas se esgotem.
    Sempre preserva o menor resultado obtido em `t_out`.

    Quando aggressive_selected=True (página marcada no front), usa o
    mesmo preset aplicado no modo 72 DPI.
    """
    target_limit_kb = LIMITE_PAGINA_KB
    reduction_ladder = [_PERFIL_72_DPI] if aggressive_selected else _ESCADA_REDUCAO

    tamanho_kb = os.path.getsize(t_out) / 1024
    if tamanho_kb <= target_limit_kb and not force_compress:
        return

    logging.info(
        f"[HP-GS] Página {page_num} com {tamanho_kb:.1f} KB | alvo {target_limit_kb} KB — "
        f"iniciando loop de recompressão ({len(reduction_ladder)} tentativas)"
    )

    for tentativa, (p_perfil, p_dpi, p_qfactor) in enumerate(reduction_ladder, start=1):
        if tamanho_kb <= target_limit_kb:
            logging.info(f"[HP-GS] Página {page_num} atingiu limite na tentativa {tentativa - 1}.")
            break

        t_recomp = os.path.join(work_dir, f"recomp_{page_num:05d}_{uid}_t{tentativa}.pdf")
        try:
            _gs_compress_page(gs_exe, t_out, t_recomp, p_perfil, p_dpi, p_qfactor)

            if not os.path.isfile(t_recomp):
                logging.warning(f"[HP-GS] Página {page_num} | tentativa {tentativa}: GS não gerou arquivo.")
                continue

            novo_kb = os.path.getsize(t_recomp) / 1024
            logging.info(
                f"[HP-GS] Página {page_num} | tentativa {tentativa} | "
                f"perfil={p_perfil} dpi={p_dpi} qf={p_qfactor} | "
                f"{tamanho_kb:.1f} KB → {novo_kb:.1f} KB"
            )

            if novo_kb < tamanho_kb:
                try:
                    os.remove(t_out)
                except OSError:
                    pass
                os.rename(t_recomp, t_out)
                tamanho_kb = novo_kb
            else:
                try:
                    os.remove(t_recomp)
                except OSError:
                    pass

                if aggressive_selected or force_compress:
                    logging.info(
                        f"[HP-GS] Página {page_num} | tentativa {tentativa} sem ganho "
                        f"({novo_kb:.1f} KB ≥ {tamanho_kb:.1f} KB) — "
                        "continuando por modo agressivo"
                    )
                    continue

                logging.warning(
                    f"[HP-GS] Página {page_num} | tentativa {tentativa} não reduziu "
                    f"({novo_kb:.1f} KB ≥ {tamanho_kb:.1f} KB) — encerrando loop"
                )
                break

        except Exception as re_err:
            logging.warning(f"[HP-GS] Página {page_num} | tentativa {tentativa} falhou: {re_err}")
            if os.path.isfile(t_recomp):
                try:
                    os.remove(t_recomp)
                except OSError:
                    pass
        finally:
            _limpar_memoria_worker()

    tamanho_final_kb = os.path.getsize(t_out) / 1024 if os.path.isfile(t_out) else 0
    if tamanho_final_kb > target_limit_kb:
        logging.warning(
            f"[HP-GS] Página {page_num} encerrou recompressão com {tamanho_final_kb:.1f} KB "
            f"(acima do limite — menor valor obtido mantido)"
        )
    else:
        logging.info(
            f"[HP-GS] Página {page_num} recomprimida com sucesso: "
            f"{tamanho_final_kb:.1f} KB ≤ {target_limit_kb} KB"
        )


def _worker_gs_page(args):
    """
    Worker de alta performance executado em processo separado.
    Pipeline por página: extrair → OCR → comprimir → recomprimir até ≤500 KB → retornar caminho.
    """
    if len(args) >= 5:
        page_idx, input_path, nivel, work_dir, selected_extra = args
    else:
        page_idx, input_path, nivel, work_dir = args
        selected_extra = False

    page_num = page_idx + 1
    uid      = uuid.uuid4().hex[:12]
    perfil   = PERFIS_GS.get(int(nivel), '/ebook')
    dpi      = PERFIS_DPI.get(int(nivel), 142)
    qfactor  = PERFIS_QFACTOR.get(int(nivel), 0.70)

    # Páginas marcadas em compressão extra usam o mesmo perfil de 72 DPI.
    if selected_extra:
        perfil, dpi, qfactor = _PERFIL_72_DPI

    t_raw = os.path.join(work_dir, f"raw_{page_num:05d}_{uid}.pdf")
    t_ocr = os.path.join(work_dir, f"ocr_{page_num:05d}_{uid}.pdf")
    t_out = os.path.join(work_dir, f"out_{page_num:05d}_{uid}.pdf")

    doc_fitz = None
    doc_pike = None

    try:
        gs_exe     = localizar_gs()
        mem_inicio = _get_memoria_processo()

        # 1. Extração de página
        try:
            with fitz.open(input_path) as doc_fitz:
                with pikepdf.open(input_path) as src:
                    out = pikepdf.Pdf.new()
                    out.pages.append(src.pages[page_idx])
                    out.save(t_raw)
                    del out
        except Exception as triage_err:
            logging.warning(f"[HP-GS] PyMuPDF falhou pág {page_num}, fallback GS: {triage_err}")
            _gs_extract_page(gs_exe, input_path, page_num, t_raw)
        finally:
            if doc_fitz is not None:
                try: doc_fitz.close()
                except: pass
            if doc_pike is not None:
                try: doc_pike.close()
                except: pass
            _limpar_memoria_worker()

        if not os.path.isfile(t_raw):
            raise FileNotFoundError(f"Extração não gerou arquivo: {t_raw}")

        # 2. OCR obrigatório
        logging.info(f"[HP-GS] Página {page_num}: OCR obrigatório ativado")
        ocr(t_raw, t_ocr, pages=None)
        _limpar_memoria_worker()

        # 3. Compressão inicial
        source_for_compress = t_ocr if os.path.isfile(t_ocr) else t_raw
        try:
            _gs_compress_page(gs_exe, source_for_compress, t_out, perfil, dpi, qfactor)
        except Exception as gs_err:
            logging.warning(f"[HP-GS] Compressão falhou na página {page_num}, usando fonte: {gs_err}")
            shutil.copy(source_for_compress, t_out)
        finally:
            _limpar_memoria_worker()

        if not os.path.isfile(t_out):
            raise FileNotFoundError(f"Compressão não gerou arquivo: {t_out}")

        # 4. Loop de recompressão persistente.
        _recomprimir_ate_limite(
            gs_exe,
            t_out,
            page_num,
            uid,
            work_dir,
            aggressive_selected=selected_extra,
            force_compress=selected_extra,
        )

        mem_final      = _get_memoria_processo()
        tamanho_final_kb = os.path.getsize(t_out) / 1024
        logging.debug(
            f"[HP-GS] Página {page_num} | {tamanho_final_kb:.1f} KB | "
            f"Memória: {mem_inicio:.1f} MB → {mem_final:.1f} MB"
        )

        return {"page": page_idx, "pdf": t_out, "success": True, "error": None}

    except Exception as e:
        logging.error(f"[HP-GS] Erro fatal na página {page_num}: {e}")
        return {"page": page_idx, "pdf": None, "success": False, "error": str(e)}

    finally:
        if doc_fitz is not None:
            try: doc_fitz.close()
            except: pass
        if doc_pike is not None:
            try: doc_pike.close()
            except: pass
        for f in [t_raw, t_ocr]:
            if os.path.isfile(f) and f != t_out:
                try: os.remove(f)
                except OSError: pass
        _limpar_memoria_worker()


def _gs_merge_hp(pdf_fragments, output_path, perfil="/ebook", dpi=170, qfactor=0.80):
    """
    Merge final de todos os fragmentos via Ghostscript com flags Xeon.

    Flags -dPassThroughJPEGImages e -dPassThroughJPXImages impedem que o GS
    re-encode imagens já comprimidas individualmente, preservando o trabalho
    da etapa _recomprimir_ate_limite e evitando inflação de tamanho por página.
    """
    gs_exe = localizar_gs()
    cmd = [
        gs_exe,
        "-dBATCH", "-dNOPAUSE", "-dQUIET", "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.5",
        f"-dPDFSETTINGS={perfil}",
        f"-dNumRenderingThreads={GS_RENDERING_THREADS}",
        f"-dBufferSpace={GS_BUFFER_SPACE}",
        "-dAutoRotatePages=/None",
        "-dEmbedAllFonts=true",
        "-dSubsetFonts=true",
        # ── Não re-encodar imagens já comprimidas por página ──
        "-dPassThroughJPEGImages=true",
        "-dPassThroughJPXImages=true",
        # ── Downsample só se necessário (acima do DPI alvo) ──
        "-dDownsampleColorImages=true",
        "-dColorImageDownsampleType=/Bicubic",
        f"-dColorImageResolution={dpi}",
        "-dDownsampleGrayImages=true",
        "-dGrayImageDownsampleType=/Bicubic",
        f"-dGrayImageResolution={dpi}",
        "-dDownsampleMonoImages=true",
        "-dMonoImageDownsampleType=/Bicubic",
        f"-dMonoImageResolution={dpi}",
        "-dAutoFilterColorImages=false",
        "-dColorImageFilter=/DCTEncode",
        "-dAutoFilterGrayImages=false",
        "-dGrayImageFilter=/DCTEncode",
        f"-sOutputFile={output_path}",
        "-c",
        f"<< /ColorACSImageDict << /QFactor {qfactor} /Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> /GrayACSImageDict << /QFactor {qfactor} /Blend 1 /HSamples [1 1 1 1] /VSamples [1 1 1 1] >> >> setdistillerparams",
        "-f",
    ] + pdf_fragments

    logging.info(f"[HP-GS] Merge: {len(pdf_fragments)} fragmentos → {output_path}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(
            f"GS merge falhou (exit {result.returncode}): {result.stderr.strip()[:300]}"
        )


def _verificar_paginas_pos_merge(merged_path, work_dir, gs_exe):
    """
    Relê o PDF merged página a página e recomprime qualquer uma que ainda
    exceda LIMITE_PAGINA_KB. Necessário porque o merge pode re-encodar imagens
    com qualidade maior que o fragmento individual tinha após _recomprimir_ate_limite.

    Estratégia:
        1. Extrai cada página do merged com pikepdf para um fragmento temporário
        2. Mede o tamanho do fragmento isolado
        3. Se > 500 KB, aplica _recomprimir_ate_limite nesse fragmento
        4. Reconstrói o PDF final substituindo as páginas grandes

    Retorna True se houve alguma recompressão (PDF precisa ser reconstruído).
    """
    paginas_grandes = []

    with pikepdf.open(merged_path) as src:
        total = len(src.pages)
        for i in range(total):
            tmp = os.path.join(work_dir, f"chk_{i:05d}.pdf")
            try:
                out = pikepdf.Pdf.new()
                out.pages.append(src.pages[i])
                out.save(tmp)
                kb = os.path.getsize(tmp) / 1024
                if kb > LIMITE_PAGINA_KB:
                    paginas_grandes.append((i, kb))
            finally:
                if os.path.isfile(tmp):
                    os.remove(tmp)

    if not paginas_grandes:
        logging.info("[HP-GS] Verificação pós-merge: todas as páginas dentro do limite.")
        return False

    logging.warning(
        f"[HP-GS] Verificação pós-merge: {len(paginas_grandes)} página(s) acima de "
        f"{LIMITE_PAGINA_KB} KB após merge — recomprimindo: "
        + ", ".join(f"pág {i+1}={kb:.0f}KB" for i, kb in paginas_grandes)
    )

    # Recomprime fragmentos grandes e reconstrói o PDF
    frags_dir = os.path.join(work_dir, "postmerge_frags")
    os.makedirs(frags_dir, exist_ok=True)
    paginas_grandes_idx = {i for i, _ in paginas_grandes}
    frag_paths = []

    with pikepdf.open(merged_path) as src:
        for i in range(total):
            uid  = uuid.uuid4().hex[:8]
            frag = os.path.join(frags_dir, f"frag_{i:05d}_{uid}.pdf")
            out  = pikepdf.Pdf.new()
            out.pages.append(src.pages[i])
            out.save(frag)

            if i in paginas_grandes_idx:
                _recomprimir_ate_limite(gs_exe, frag, i + 1, uid, frags_dir)
                kb_final = os.path.getsize(frag) / 1024
                logging.info(f"[HP-GS] Pós-merge pág {i+1}: recomprimida para {kb_final:.1f} KB")

            frag_paths.append(frag)
            _limpar_memoria_worker()

    # Reconstrói o merged com pikepdf (sem re-encode de imagens)
    reconstruido = merged_path + ".rebuilt.pdf"
    with pikepdf.Pdf.new() as dest:
        for frag in frag_paths:
            with pikepdf.open(frag) as src:
                dest.pages.extend(src.pages)
        dest.save(reconstruido)

    os.replace(reconstruido, merged_path)

    # Limpeza dos fragmentos temporários
    for frag in frag_paths:
        try:
            os.remove(frag)
        except OSError:
            pass
    try:
        os.rmdir(frags_dir)
    except OSError:
        pass

    logging.info("[HP-GS] Reconstrução pós-merge concluída.")
    return True


def processar_pdf_custom(
    input_path,
    output_path,
    config_map,
    callback=None,
    check_cancelled=None,
    extra_compress_pages=None,
):
    """
    Motor GS de alta performance com ProcessPoolExecutor.
    """
    doc_fitz = None
    work_dir = None

    try:
        with fitz.open(input_path) as doc_fitz:
            total = len(doc_fitz)

        if total == 0:
            shutil.copy2(input_path, output_path)
            return

        _limpar_memoria_worker()

        session_id = uuid.uuid4().hex[:16]
        base_dir   = temp_dir()
        work_dir   = os.path.join(base_dir, f"gs_session_{session_id}")
        os.makedirs(work_dir, exist_ok=True)

        mem_inicio = _get_memoria_processo()
        logging.info(
            f"[HP-GS] Processando {total} páginas com 8 workers | "
            f"ramdisk={work_dir} | Memória inicial: {mem_inicio:.1f} MB"
        )

        selected_pages = set()
        for p in (extra_compress_pages or []):
            try:
                page = int(p)
            except Exception:
                continue
            if 1 <= page <= total:
                selected_pages.add(page)

        if selected_pages:
            logging.info(
                f"[HP-GS] Compressão extra (preset 72 DPI) habilitada para "
                f"{len(selected_pages)} página(s): {sorted(selected_pages)}"
            )

        try:
            tarefas = [
                (
                    i,
                    input_path,
                    config_map.get(str(i), 3),
                    work_dir,
                    (i + 1) in selected_pages,
                )
                for i in range(total)
            ]

            results_map  = {}
            completed    = 0
            failed_pages = []

            max_workers_safe = max(4, min(8, MAX_WORKERS - 4))

            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers_safe) as executor:
                future_to_page = {
                    executor.submit(_worker_gs_page, t): t[0]
                    for t in tarefas
                }

                for future in concurrent.futures.as_completed(future_to_page):
                    if check_cancelled and check_cancelled():
                        executor.shutdown(wait=False, cancel_futures=True)
                        logging.info("[HP-GS] Processamento cancelado pelo usuário.")
                        return

                    page_idx = future_to_page[future]
                    try:
                        result = future.result(timeout=300)
                        results_map[page_idx] = result
                        if not result["success"]:
                            failed_pages.append(page_idx)
                            logging.warning(f"[HP-GS] Página {page_idx} falhou: {result['error']}")
                    except Exception as e:
                        failed_pages.append(page_idx)
                        results_map[page_idx] = {
                            "page": page_idx, "pdf": None,
                            "success": False, "error": str(e),
                        }
                        logging.error(f"[HP-GS] Exceção no worker da página {page_idx}: {e}")
                    finally:
                        completed += 1
                        if callback:
                            try: callback(completed - 1, total)
                            except Exception: pass
                        _limpar_memoria_worker()
                        if completed % 50 == 0:
                            mem_atual = _get_memoria_processo()
                            logging.info(
                                f"[HP-GS] Progresso: {completed}/{total} páginas | "
                                f"Memória: {mem_atual:.1f} MB | Falhas: {len(failed_pages)}"
                            )

            mem_pos_workers = _get_memoria_processo()
            logging.info(f"[HP-GS] Término workers: {completed}/{total} | Memória: {mem_pos_workers:.1f} MB")

            pdf_fragments = []
            for page_idx in range(total):
                if page_idx in results_map:
                    r = results_map[page_idx]
                    if r and r.get("pdf") and os.path.isfile(r["pdf"]):
                        pdf_fragments.append(r["pdf"])

            if not pdf_fragments:
                first_error = "desconhecido"
                for r in results_map.values():
                    if r and not r.get("success") and r.get("error"):
                        first_error = r["error"]
                        break
                raise RuntimeError(
                    f"Nenhuma página processada. Falhas: {len(failed_pages)}/{total}. "
                    f"Primeiro erro: {first_error}"
                )

            if failed_pages:
                logging.warning(f"[HP-GS] {len(failed_pages)} página(s) falharam: {failed_pages}")

            del results_map
            _limpar_memoria_worker()

            logging.info(f"[HP-GS] Merge: {len(pdf_fragments)} fragmentos | Memória: {_get_memoria_processo():.1f} MB")

            niveis           = [int(config_map.get(str(i), 3)) for i in range(total)]
            nivel_dominante  = max(set(niveis), key=niveis.count)
            perfil_merge     = PERFIS_GS.get(nivel_dominante, '/ebook')
            dpi_merge        = PERFIS_DPI.get(nivel_dominante, 150)
            qfactor_merge    = PERFIS_QFACTOR.get(nivel_dominante, 0.76)

            merged_tmp = os.path.join(work_dir, "merged_final.pdf")
            _gs_merge_hp(pdf_fragments, merged_tmp, perfil_merge, dpi_merge, qfactor_merge)

            if not os.path.isfile(merged_tmp):
                raise RuntimeError("[HP-GS] Merge final não gerou arquivo.")

            _limpar_memoria_worker()

            # ── Verificação pós-merge: recomprime páginas que o GS inflou ──
            gs_exe = localizar_gs()
            _verificar_paginas_pos_merge(merged_tmp, work_dir, gs_exe)
            _limpar_memoria_worker()

            # ── Compressão extra se o arquivo ainda for grande ──
            merged_size_mb = os.path.getsize(merged_tmp) / (1024 * 1024)
            if merged_size_mb > EXTRA_COMPRESS_THRESHOLD_MB:
                extra_dpi = max(30, int(dpi_merge * 1.2))
                extra_tmp = os.path.join(work_dir, "merged_extra.pdf")
                logging.info(
                    f"[HP-GS] Output {merged_size_mb:.1f} MB > {EXTRA_COMPRESS_THRESHOLD_MB} MB, "
                    f"compressão extra com DPI={extra_dpi} e /screen"
                )
                _gs_merge_hp([merged_tmp], extra_tmp, "/screen", extra_dpi, 0.30)
                if os.path.isfile(extra_tmp):
                    extra_size_mb = os.path.getsize(extra_tmp) / (1024 * 1024)
                    if extra_size_mb < merged_size_mb:
                        os.replace(extra_tmp, merged_tmp)
                        logging.info(f"[HP-GS] Compressão extra: {merged_size_mb:.1f} MB → {extra_size_mb:.1f} MB")
                    else:
                        os.remove(extra_tmp)
                        logging.info("[HP-GS] Compressão extra não reduziu, mantendo original")

            _limpar_memoria_worker()

            del pdf_fragments
            _limpar_memoria_worker()

            shutil.move(merged_tmp, output_path)

            final_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            mem_final     = _get_memoria_processo()
            logging.info(
                f"[HP-GS] SUCESSO | {total} páginas | {len(failed_pages)} falhas | "
                f"{final_size_mb:.2f} MB | perfil={perfil_merge} | "
                f"Memória: {mem_inicio:.1f} MB → {mem_final:.1f} MB"
            )

        except Exception:
            logging.exception("[HP-GS] Falha crítica no processamento")
            raise

        finally:
            if work_dir and os.path.isdir(work_dir):
                try:
                    for root, dirs, files in os.walk(work_dir, topdown=False):
                        for f in files:
                            try: os.remove(os.path.join(root, f))
                            except: pass
                    shutil.rmtree(work_dir, ignore_errors=True)
                    logging.info(f"[HP-GS] Diretório temporário removido: {work_dir}")
                except Exception as e:
                    logging.warning(f"[HP-GS] Falha ao limpar {work_dir}: {e}")
            _limpar_memoria_worker()

    finally:
        if doc_fitz is not None:
            try: doc_fitz.close()
            except: pass
        _limpar_memoria_worker()