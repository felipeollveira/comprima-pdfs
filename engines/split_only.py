"""
Função para dividir PDF em volumes sem aplicar OCR.
Usado quando o OCR já foi aplicado no PDF completo.
"""
import io
import pathlib
import pikepdf
import logging
import subprocess
import shutil
from engines.ramdisk import temp_dir
from engines.locate_gs import localizar_gs
from engines.force_ocr import get_paginas_necessitam_ocr, ocr
from engines.constants import (
    SAFETY_MARGIN,
)


def _compress_volume_gs(input_path: str, output_path: str) -> bool:
    """
    Compressão corretiva via Ghostscript quando volume excede limite pós-OCR.
    Retorna True se conseguiu comprimir, False caso contrário.
    """
    try:
        gs = localizar_gs()
        cmd = [
            gs,
            "-dBATCH", "-dNOPAUSE", "-dQUIET", "-dSAFER",
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.5",
            "-dPDFSETTINGS=/screen",
            "-dDownsampleColorImages=true",
            "-dColorImageResolution=100",
            "-dDownsampleGrayImages=true",
            "-dGrayImageResolution=100",
            "-dDownsampleMonoImages=true",
            "-dMonoImageResolution=100",
            f"-sOutputFile={output_path}",
            input_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return result.returncode == 0
    except Exception as e:
        logging.warning(f"[split_only] Compressão corretiva falhou: {e}")
        return False


# ---------------------------------------------------------------------------
# Helpers de peso
# ---------------------------------------------------------------------------

def _pdf_to_bytes(pdf: pikepdf.Pdf) -> int:
    """Salva PDF em BytesIO e retorna tamanho em bytes. Sem I/O em disco."""
    buf = io.BytesIO()
    pdf.save(buf)
    return buf.tell()


def _calcular_pesos_paginas(pdf: pikepdf.Pdf) -> list[int]:
    """
    Calcula o custo INCREMENTAL real de cada página.

    Estratégia: mede o delta do arquivo ao adicionar cada página a um PDF
    crescente. Isso reflete como o volume final realmente cresce — recursos
    compartilhados (fontes, imagens referenciadas em múltiplas páginas) são
    contados apenas uma vez, assim como acontece no arquivo real.

    Por que o PDF isolado por página dá valor errado:
    • Cada PDF isolado carrega header + xref + todos os recursos da página.
    • Quando o browser ou o SO mostra o tamanho do volume dividido, os
      recursos compartilhados existem uma única vez → tamanho menor.
    • Somar PDFs isolados sempre superestima o tamanho real do volume.

    Complemento: para páginas com imagens únicas grandes (não compartilhadas),
    o delta incremental pode subestimar levemente. Por isso usamos
    max(delta_incremental, tamanho_isolado − overhead_vazio) como piso.
    """
    total = len(pdf.pages)
    empty_overhead = _pdf_to_bytes(pikepdf.Pdf.new())

    # Pesos isolados (via BytesIO — sem disco)
    pesos_isolados = []
    for idx in range(total):
        single = pikepdf.Pdf.new()
        single.pages.append(pdf.pages[idx])
        pesos_isolados.append(_pdf_to_bytes(single) - empty_overhead)

    # Pesos incrementais
    growing = pikepdf.Pdf.new()
    prev_size = _pdf_to_bytes(growing)
    pesos_incrementais = []
    for idx in range(total):
        growing.pages.append(pdf.pages[idx])
        new_size = _pdf_to_bytes(growing)
        delta = max(new_size - prev_size, 512)  # piso de 512 B por página
        pesos_incrementais.append(delta)
        prev_size = new_size

    # Usa o máximo: conservador sem superestimar como o método isolado puro
    pesos = [
        max(inc, iso)
        for inc, iso in zip(pesos_incrementais, pesos_isolados)
    ]

    logging.info(
        f"Pesos por página (KB): {[f'{w // 1024}' for w in pesos]}"
    )
    return pesos


# ---------------------------------------------------------------------------
# Função principal
# ---------------------------------------------------------------------------

def split_pdf_only(
    pdf_path,
    out_dir,
    max_mb,
    on_page=None,
    on_volume=None,
    on_ocr=None,
    check_cancelled=None,
):
    logging.info(f"Iniciando divisão em volumes: {pdf_path}")
    out_dir = pathlib.Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    max_bytes_safe = int(float(max_mb) * 1024 * 1024 * SAFETY_MARGIN)
    max_bytes_hard = int(float(max_mb) * 1024 * 1024)
    logging.info(
        f"[split_only] Limite seguro ({SAFETY_MARGIN*100:.0f}%): "
        f"{max_bytes_safe / (1024*1024):.2f} MB  |  Hard: {max_mb} MB"
    )

    ram_dir = pathlib.Path(temp_dir())

    try:
        with pikepdf.open(pdf_path) as pdf:
            total = len(pdf.pages)

            logging.info("Pré-calculando pesos das páginas…")
            page_weights = _calcular_pesos_paginas(pdf)

            vol, i, current_page = 1, 0, 0

            while i < total:
                if check_cancelled and check_cancelled():
                    return {"cancelled": True, "total_pages": total}

                final_out = out_dir / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"
                ram_out = ram_dir / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"

                part = pikepdf.Pdf.new()
                added = 0
                accumulated = 0  # estimativa acumulada do volume atual

                while i < total:
                    if check_cancelled and check_cancelled():
                        return {"cancelled": True, "total_pages": total}

                    next_weight = page_weights[i]
                    projected = accumulated + next_weight

                    # ── Guarda de pré-adição ──────────────────────────────────
                    # Se a projeção já passa do limite seguro e já há páginas,
                    # confirma com I/O real antes de decidir.
                    if added > 0 and projected > max_bytes_safe:
                        actual = _pdf_to_bytes(part)
                        if actual > max_bytes_safe:
                            # Não cabe: fecha o volume aqui.
                            break
                        # Estimativa estava errada; recalibra e continua.
                        accumulated = actual

                    # ── Adiciona a página ─────────────────────────────────────
                    part.pages.append(pdf.pages[i])
                    added += 1
                    accumulated += next_weight
                    i += 1
                    current_page += 1
                    if on_page:
                        on_page(current_page, total)

                    # ── Checagem periódica com I/O real ───────────────────────
                    # Apenas quando estimativa > 75% do limite (evita saves
                    # desnecessários nas páginas iniciais de cada volume).
                    if accumulated > max_bytes_safe * 0.75:
                        actual = _pdf_to_bytes(part)
                        if actual > max_bytes_safe:
                            # Passou do limite: faz backtrack de 1 página.
                            del part.pages[-1]
                            i -= 1
                            current_page -= 1
                            added -= 1
                            accumulated -= next_weight
                            break
                        # Recalibra estimativa com valor real medido.
                        accumulated = actual

                # ── Salva volume em disco ─────────────────────────────────────
                buf = io.BytesIO()
                part.save(buf)
                ram_out.write_bytes(buf.getvalue())
                size_mb = len(buf.getvalue()) / (1024 * 1024)
                logging.info(f"Volume {vol}: {added} páginas — {size_mb:.2f} MB")
                if on_volume:
                    on_volume(vol, added, size_mb)

                # ── OCR ───────────────────────────────────────────────────────
                try:
                    if on_ocr:
                        on_ocr(vol)
                    paginas_ocr = get_paginas_necessitam_ocr(str(ram_out))
                    if paginas_ocr is None:
                        ocr(str(ram_out), str(ram_out))
                    elif paginas_ocr:
                        ocr(str(ram_out), str(ram_out), pages=paginas_ocr)
                        logging.info(
                            f"Volume {vol}: OCR em {len(paginas_ocr)}/{added} páginas"
                        )
                    else:
                        logging.info(f"Volume {vol}: OCR não necessário")
                except Exception as e:
                    logging.warning(f"Falha no OCR do volume {vol}: {e}")

                # ── Verificação pós-OCR ───────────────────────────────────────
                post_ocr_size = ram_out.stat().st_size
                if post_ocr_size > max_bytes_hard:
                    logging.warning(
                        f"Volume {vol} pós-OCR: {post_ocr_size/(1024*1024):.2f} MB "
                        f"> {max_mb} MB — aplicando compressão corretiva…"
                    )
                    compressed = ram_dir / f"{pdf_path.stem}_VOL_{vol:02d}_c.pdf"
                    if _compress_volume_gs(str(ram_out), str(compressed)):
                        cs = compressed.stat().st_size
                        if cs < post_ocr_size:
                            logging.info(
                                f"Volume {vol}: {post_ocr_size/(1024*1024):.2f} MB "
                                f"→ {cs/(1024*1024):.2f} MB"
                            )
                            ram_out.unlink()
                            compressed.rename(ram_out)
                            post_ocr_size = cs
                        else:
                            compressed.unlink(missing_ok=True)
                            logging.warning(
                                f"Volume {vol}: compressão não reduziu tamanho"
                            )

                    if post_ocr_size > max_bytes_hard:
                        logging.error(
                            f"Volume {vol}: ainda {post_ocr_size/(1024*1024):.2f} MB "
                            f"após compressão. Reduza max_mb ou páginas por volume."
                        )

                shutil.move(str(ram_out), str(final_out))
                vol += 1

    finally:
        # Garante limpeza do temp dir mesmo em caso de exceção
        shutil.rmtree(str(ram_dir), ignore_errors=True)

    return {"cancelled": False, "total_pages": total}