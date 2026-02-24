"""
Função para dividir PDF em volumes sem aplicar OCR.
Usado quando o OCR já foi aplicado no PDF completo.
"""
import pathlib
import pikepdf
import logging
import os
import subprocess
import shutil
from engines.ramdisk import temp_dir
from engines.locate_gs import localizar_gs


def split_pdf_only(pdf_path, out_dir, max_mb, on_page=None, on_volume=None, check_cancelled=None):
    """
    Divide o PDF em volumes respeitando o limite de MB, SEM aplicar OCR.
    Usado quando o OCR já foi aplicado anteriormente.
    """
    logging.info(f"Iniciando divisão em volumes (sem OCR): {pdf_path}")
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    max_bytes = int(float(max_mb) * 1024 * 1024)
    ram_dir = temp_dir()

    with pikepdf.open(pdf_path) as pdf:
        total = len(pdf.pages)
        vol, i = 1, 0
        current_page = 0

        while i < total:
            part = pikepdf.Pdf.new()
            added = 0
            final_out = out_dir / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"
            # Usa RAM Disk para escrita intermediária rápida
            ram_out = pathlib.Path(ram_dir) / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"

            while i < total:
                if check_cancelled and check_cancelled():
                    if ram_out.exists():
                        ram_out.unlink()
                    return {"cancelled": True, "total_pages": total}

                part.pages.append(pdf.pages[i])
                added += 1
                i += 1
                current_page += 1

                if on_page:
                    on_page(current_page, total)

                # Salva no RAM Disk para verificar tamanho
                part.save(str(ram_out))
                if ram_out.stat().st_size > max_bytes and added > 1:
                    # Volume cheio, remove a última página
                    del part.pages[-1]
                    part.save(str(ram_out))
                    i -= 1
                    current_page -= 1
                    break

            # ── Compressão GS do volume ────────────────────────────────
            # Cada volume é re-comprimido para garantir o menor tamanho
            try:
                gs_exe = localizar_gs()
                gs_out = pathlib.Path(ram_dir) / f"gs_{pdf_path.stem}_VOL_{vol:02d}.pdf"
                cmd = [
                    gs_exe,
                    "-dBATCH", "-dNOPAUSE", "-dQUIET", "-dSAFER",
                    "-sDEVICE=pdfwrite",
                    "-dCompatibilityLevel=1.4",
                    "-dPDFSETTINGS=/screen",
                    "-dDownsampleColorImages=true",
                    "-dColorImageDownsampleType=/Bicubic",
                    "-dColorImageResolution=100",
                    "-dDownsampleGrayImages=true",
                    "-dGrayImageDownsampleType=/Bicubic",
                    "-dGrayImageResolution=100",
                    "-dDownsampleMonoImages=true",
                    "-dMonoImageResolution=100",
                    "-dAutoFilterColorImages=false",
                    "-dColorImageFilter=/DCTEncode",
                    "-dAutoFilterGrayImages=false",
                    "-dGrayImageFilter=/DCTEncode",
                    "-c", ".setpdfwrite << /ColorACSImageDict << /QFactor 0.76 /Blend 1 /HSamples [2 1 1 2] /VSamples [2 1 1 2] >> /GrayACSImageDict << /QFactor 0.76 /Blend 1 /HSamples [2 1 1 2] /VSamples [2 1 1 2] >> >> setdistillerparams",
                    "-f",
                    f"-sOutputFile={str(gs_out)}",
                    str(ram_out)
                ]
                subprocess.run(cmd, check=True, timeout=120)
                if gs_out.exists() and gs_out.stat().st_size < ram_out.stat().st_size:
                    ram_out.unlink()
                    gs_out.rename(ram_out)
                    logging.info(f"Volume {vol}: compressão GS aplicada com sucesso")
                elif gs_out.exists():
                    gs_out.unlink()
            except Exception as gs_err:
                logging.warning(f"Compressão GS do volume {vol} falhou (usando pikepdf): {gs_err}")

            # Log do volume criado
            size_mb = ram_out.stat().st_size / (1024 * 1024)
            logging.info(f"Volume {vol} criado com {added} páginas ({size_mb:.2f} MB)")
            if on_volume:
                on_volume(vol, added, size_mb)

            # Move do RAM Disk para destino final
            shutil.move(str(ram_out), str(final_out))
            vol += 1

    return {"cancelled": False, "total_pages": total}