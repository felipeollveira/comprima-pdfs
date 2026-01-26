"""
ATENÇÃO: Se o usuário selecionar OCR para páginas específicas, chame apenas a função ocr().
NÃO chame split_volumes nesse caso. split_volumes é para divisão de volumes sem OCR isolado.
A lógica de decisão deve estar no backend (ex: app.py).
"""

import pathlib
import subprocess
import os
import logging

import fitz
import pikepdf

DPI = 200
LANG = "por+eng"
MAX_MB = 4.8
JPEG_QUALITY = 60


def mb(b): return b / (1024 * 1024)

def run(cmd):
    logging.info(f"Executando comando: {' '.join(map(str, cmd))}")
    try:
        subprocess.run(cmd, check=True)
        logging.info("Comando executado com sucesso.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao executar comando: {e}")
        raise


def raster_grayscale(input_pdf, out_pdf, dpi):
    logging.info(f"Rasterizando PDF em tons de cinza: {input_pdf} -> {out_pdf} @ {dpi}dpi")
    src = fitz.open(str(input_pdf))
    dst = fitz.open()
    zoom = dpi / 72.0
    mat = fitz.Matrix(zoom, zoom)

    try:
        for page in src:
            pix = page.get_pixmap(matrix=mat, colorspace=fitz.csGRAY, alpha=False)
            rect = fitz.Rect(0, 0, pix.width, pix.height)
            p = dst.new_page(width=rect.width, height=rect.height)
            p.insert_image(
                rect,
                stream=pix.tobytes("jpeg"),
                keep_proportion=True
            )
        dst.save(out_pdf, deflate=True)
        logging.info(f"Rasterização concluída: {out_pdf}")
    except Exception as e:
        logging.error(f"Erro ao rasterizar PDF: {e}")
        raise
    finally:
        dst.close()
        src.close()


def ocr(input_pdf, out_pdf, pages=None):
    cmd = [
        "ocrmypdf",
        "--output-type", "pdf",      # Muda de PDF/A para PDF comum (reduz tamanho)
        "--optimize", "3",           # Nível 3: Compressão JBIG2 agressiva
        "--pdf-renderer", "sandwich",
        "--force-ocr",                  
        "--deskew",
        "--clean",                   # Remove ruído da imagem para melhorar compressão
        "--rotate-pages",
        "--jpeg-quality", str(JPEG_QUALITY),
        "--jbig2-lossy",             # Ativado (agora que você tem jbig2enc)
        "-l", LANG,
        str(input_pdf),
        str(out_pdf)
    ]
    if pages:
        # Se for um volume já cortado, não passamos o range original
        # Apenas passamos se o input for o PDF original completo
        cmd.extend(["--pages", str(pages)])
    
    run(cmd)

def split_volumes(pdf_path, out_dir, max_mb):
    logging.info(f"Iniciando divisão em volumes: {pdf_path} -> {out_dir} (max_mb={max_mb})")
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    max_bytes = int(max_mb * 1024 * 1024)
    try:
        with pikepdf.open(pdf_path) as pdf:
            total = len(pdf.pages)
            vol = 1
            i = 0

            while i < total:
                part = pikepdf.Pdf.new()
                added = 0
                out = out_dir / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"

                start_page = i + 1  # 1-based
                while i < total:
                    part.pages.append(pdf.pages[i])
                    added += 1
                    part.save(out)

                    if out.stat().st_size > max_bytes:
                        if added == 1:
                            i += 1
                        else:
                            part2 = pikepdf.Pdf.new() # type: ignore
                            for k in range(added - 1):
                                part2.pages.append(pdf.pages[i - (added - 1) + k])
                            part2.save(out)
                        break

                    i += 1

                end_page = start_page + added - 1
                if added > 0 and end_page >= start_page:
                    # Chame o OCR apenas se o intervalo for válido
                    try:
                        ocr(out, out)
                    except Exception as e:
                        logging.error(f"Erro ao rodar OCR no volume {vol}: {e}")

                logging.info(f"Volume {vol}: {out.name} ({mb(out.stat().st_size):.2f} MB)")
                vol += 1
        logging.info("Divisão em volumes concluída.")
    except Exception as e:
        logging.error(f"Erro na divisão em volumes: {e}")
        raise
