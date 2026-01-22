import pathlib
import subprocess
import os

import fitz
import pikepdf

DPI = 200
LANG = "por+eng"
MAX_MB = 5.0
JPEG_QUALITY = 60


def mb(b): return b / (1024 * 1024)

def run(cmd):
    subprocess.run(cmd, check=True)


def raster_grayscale(input_pdf, out_pdf, dpi):
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
    finally:
        dst.close()
        src.close()


def ocr(input_pdf, out_pdf):
    cmd = [
        "ocrmypdf",
        "--force-ocr",
        "--deskew",
        "--rotate-pages",
        "--optimize", "1",
        "--jpeg-quality", str(JPEG_QUALITY),
        "-l", LANG,
        str(input_pdf),
        str(out_pdf)
    ]
    run(cmd)

def split_volumes(pdf_path, out_dir, max_mb):
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    max_bytes = int(max_mb * 1024 * 1024)
    
    with pikepdf.open(pdf_path) as pdf:
        total = len(pdf.pages)
        vol = 1
        i = 0

        while i < total:
            part = pikepdf.Pdf.new()
            added = 0
            out = out_dir / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"

            
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

            print(f"Volume {vol}: {out.name} ({mb(out.stat().st_size):.2f} MB)")
            vol += 1
