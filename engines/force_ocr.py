import pathlib
import subprocess
import os
import logging
import fitz  # PyMuPDF
import pikepdf
import shutil
from engines.ramdisk import temp_dir

# Configurações globais
LANG = "por+eng"
MAX_MB = 4.8
LIMITE_CHARS_OCR = 200 

def run(cmd):
    """Executa o comando capturando erros detalhados do stderr."""
    logging.info(f"Executando: {' '.join(map(str, cmd))}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro no OCRmyPDF (Status {e.returncode}):")
        logging.error(f"STDERR: {e.stderr}")
        raise

def get_paginas_necessitam_ocr(input_pdf, limite=LIMITE_CHARS_OCR):
    """Analisa o PDF e identifica páginas com imagens dominantes e pouco texto."""
    paginas_alvo = []
    try:
        doc = fitz.open(str(input_pdf))
        for i, page in enumerate(doc):
            texto = page.get_text().strip()
            palavra_count = len(texto.split())
            
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
            
            # Precisa OCR se: tem imagem dominante E pouco texto
            if tem_imagem and imagem_dominante and palavra_count < 50:
                paginas_alvo.append(i + 1)
        doc.close()
    except Exception as e:
        logging.error(f"Erro ao analisar texto: {e}")
        return None 
    return paginas_alvo

def ocr(input_pdf, out_pdf, pages=None):
    """Executa OCR seletivo sem duplicar argumentos de arquivo."""
    # Se a triagem indicou que não precisa de OCR, apenas copia
    if pages is not None and len(pages) == 0:
        if str(input_pdf) != str(out_pdf):
            import shutil
            shutil.copy(str(input_pdf), str(out_pdf))
        return

    # Usamos caminhos absolutos para evitar problemas em threads
    abs_input = os.path.abspath(input_pdf)
    abs_output = os.path.abspath(out_pdf)

    # Base do comando com argumentos limpos
    cmd = [
        "ocrmypdf",
        "--output-type", "pdf",
        "--optimize", "2",
        "--force-ocr",
        "--deskew",
        "--rotate-pages",
        "--jobs", "10",
        "--invalidate-digital-signatures",
        "-l", str(LANG)
    ]
    
    # Adiciona pypáginas APENAS se houver uma lista específica (uso em volumes)
    if pages and isinstance(pages, list):
        cmd.extend(["--pages", ",".join(map(str, pages))])
    
    # Adiciona os arquivos de entrada e saída uma única vez
    cmd.append(abs_input)
    cmd.append(abs_output)
    
    run(cmd)

def split_volumes(pdf_path, out_dir, max_mb, on_page=None, on_volume=None, on_ocr=None, check_cancelled=None):
    """Divide o PDF em volumes respeitando o limite de MB."""
    logging.info(f"Iniciando divisão em volumes: {pdf_path}")
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    max_bytes = int(float(max_mb) * 1024 * 1024)
    ram_dir = temp_dir()  # /dev/shm para I/O rápido
    
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
                    # Limpa arquivo temporário do RAM Disk
                    if ram_out.exists():
                        ram_out.unlink()
                    return {"cancelled": True, "total_pages": total}
                part.pages.append(pdf.pages[i])
                added += 1
                i += 1
                current_page += 1
                if on_page:
                    on_page(current_page, total)
                # Salva no RAM Disk (muito mais rápido que disco)
                part.save(str(ram_out))
                if ram_out.stat().st_size > max_bytes and added > 1:
                    # Volume cheio, remove a última página adicionada
                    del part.pages[-1]
                    part.save(str(ram_out))
                    i -= 1  # Readiciona a página para o próximo volume
                    current_page -= 1
                    break
            
            # Log mais informativo
            size_mb = ram_out.stat().st_size / (1024 * 1024)
            logging.info(f"Volume {vol} criado com {added} páginas ({size_mb:.2f} MB)")
            if on_volume:
                on_volume(vol, added, size_mb)
            
            # Aplica OCR no RAM Disk (leitura e escrita em RAM)
            try:
                if on_ocr:
                    on_ocr(vol)
                ocr(str(ram_out), str(ram_out))
            except Exception as e:
                logging.warning(f"Falha no OCR do volume {vol}. O arquivo será mantido sem OCR: {e}")
            
            # Move resultado final do RAM Disk para disco
            shutil.move(str(ram_out), str(final_out))
            vol += 1

    return {"cancelled": False, "total_pages": total}