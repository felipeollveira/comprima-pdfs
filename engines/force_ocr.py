import pathlib
import subprocess
import os
import logging
import fitz  # PyMuPDF
import pikepdf
import shutil
from engines.ramdisk import temp_dir
from engines.constants import (
    MAX_PAGE_KB,
    MAX_PAGE_BYTES,
    MAX_DOC_KB,
    MAX_DOC_MB,
    MAX_DOC_MB_SAFE,
    LIMITE_CHARS_OCR,
    TESSERACT_LANG,
    MIN_IMAGE_AREA_RATIO,
)

# Aliases para compatibilidade com código existente
LANG = TESSERACT_LANG
MAX_MB = MAX_DOC_MB  # 5000 KB = 4.88 MB

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
            imagem_dominante = imagem_area_total > (page_area * MIN_IMAGE_AREA_RATIO) if page_area > 0 else False

            # Precisa OCR se: tem imagem dominante E pouco texto
            if tem_imagem and imagem_dominante and palavra_count < 50:
                paginas_alvo.append(i + 1)
        doc.close()
    except Exception as e:
        logging.error(f"Erro ao analisar texto: {e}")
        return None
    return paginas_alvo


def get_paginas_seguras_para_ocr(input_pdf, paginas_candidatas):
    """
    Filtra páginas que são seguras para OCR baseado no tamanho estimado.

    Calcula: tamanho_atual + 30% para cada página
    Se resultado > 500KB, a página é ignorada (para evitar explosão de tamanho)

    Args:
        input_pdf: caminho do PDF
        paginas_candidatas: lista de números de páginas a analisar

    Returns:
        dict com:
        - paginas_seguras: lista de páginas que podem ser processadas
        - puladas: dict com páginas puladas e motivo
    """
    if not paginas_candidatas:
        return {"paginas_seguras": [], "puladas": {}}

    OCR_OVERHEAD = 1.30  # 30% de overhead estimado
    result = {"paginas_seguras": [], "puladas": {}}

    try:
        doc = fitz.open(str(input_pdf))

        for page_num in paginas_candidatas:
            if page_num < 1 or page_num > len(doc):
                logging.warning(f"Página {page_num} inválida (fora do intervalo)")
                result["puladas"][page_num] = "página inválida"
                continue

            page = doc[page_num - 1]

            # Extrai página para estimar tamanho
            try:
                page_pdf = pathlib.Path(temp_dir()) / f"page_{page_num}_temp.pdf"
                single_page_doc = fitz.open()
                single_page_doc.insert_pdf(doc, from_page=page_num - 1, to_page=page_num - 1)
                single_page_doc.save(str(page_pdf))
                single_page_doc.close()

                actual_size = page_pdf.stat().st_size
                estimated_size = actual_size * OCR_OVERHEAD

                # Remove arquivo temporário
                page_pdf.unlink()

                if estimated_size > MAX_PAGE_BYTES:
                    logging.info(
                        f"Página {page_num} PULADA: "
                        f"tamanho atual {actual_size / 1024:.1f}KB + 30% overhead = "
                        f"{estimated_size / 1024:.1f}KB (limite: {MAX_PAGE_KB}KB)"
                    )
                    result["puladas"][page_num] = f"tamanho estimado {estimated_size / 1024:.1f}KB > {MAX_PAGE_KB}KB"
                else:
                    result["paginas_seguras"].append(page_num)
                    logging.debug(
                        f"Página {page_num} segura: "
                        f"{actual_size / 1024:.1f}KB + 30% = {estimated_size / 1024:.1f}KB"
                    )
            except Exception as e:
                logging.warning(f"Erro ao analisar tamanho da página {page_num}: {e}")
                result["puladas"][page_num] = str(e)

        doc.close()
    except Exception as e:
        logging.error(f"Erro ao processar PDF: {e}")
        return {"paginas_seguras": [], "puladas": {"erro": str(e)}}

    # Log resumo
    total_puladas = len(result["puladas"])
    total_seguras = len(result["paginas_seguras"])
    if total_puladas > 0:
        logging.info(
            f"Filtro de tamanho OCR: {total_seguras} páginas seguras, "
            f"{total_puladas} puladas por exceder 500KB"
        )

    return result

def ocr(input_pdf, out_pdf, pages=None):
    """Executa OCR seletivo sem duplicar argumentos de arquivo."""
    # OCR obrigatorio: lista vazia de paginas vira OCR em todas as paginas.
    if pages is not None and len(pages) == 0:
        pages = None

    # Usamos caminhos absolutos para evitar problemas em threads
    abs_input = os.path.abspath(input_pdf)
    abs_output = os.path.abspath(out_pdf)

    # Base do comando com argumentos limpos
    cmd = [
        "ocrmypdf",
        "--output-type", "pdf",
        "--optimize", "3",
        "--force-ocr",
        "--deskew",
        "--rotate-pages",
        "--jobs", "14",
        "--tesseract-timeout", "140",
        "--skip-big", "50",
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
    """Divide o PDF em volumes respeitando rigorosamente o limite de MB."""
    logging.info(f"Iniciando divisão em volumes: {pdf_path}")
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)

    # Usa margem de segurança para absorver overhead do OCR (~15%)
    safe_max_mb = max_mb * 0.85  # 85% do limite
    max_bytes = int(float(safe_max_mb) * 1024 * 1024)
    max_page_bytes = MAX_PAGE_BYTES
    ram_dir = temp_dir()

    logging.info(f"[split_volumes] Limite: {max_mb} MB → Limite seguro: {safe_max_mb:.2f} MB (margem 15% para OCR)")

    with pikepdf.open(pdf_path) as pdf:
        total = len(pdf.pages)
        vol, i = 1, 0
        current_page = 0

        while i < total:
            part = pikepdf.Pdf.new()
            added = 0
            final_out = out_dir / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"
            ram_out = pathlib.Path(ram_dir) / f"{pdf_path.stem}_VOL_{vol:02d}.pdf"
            page_sizes = []  # Histórico real de tamanhos por página
            last_saved_size = 0

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

                # === ESTRATÉGIA DE VERIFICAÇÃO RIGOROSA ===
                # Calcula média das últimas páginas para estimativa mais precisa
                avg_recent = (
                    sum(page_sizes[-5:]) / len(page_sizes[-5:])
                    if page_sizes else 0
                )
                estimated_size = last_saved_size + avg_recent if avg_recent > 0 else 0

                # Força verificação real se:
                # (a) primeiras 2 páginas (calibração inicial)
                # (b) estimativa passa 65% do limite (margem de segurança maior)
                # (c) a cada 10 páginas (fallback periódico mais frequente)
                # (d) SEMPRE na última página do PDF (garante flush correto)
                is_last_page = (i == total)
                should_check = (
                    added <= 2
                    or (avg_recent > 0 and estimated_size > max_bytes * 0.65)
                    or added % 10 == 0
                    or is_last_page
                )

                if should_check:
                    part.save(str(ram_out))
                    actual_size = ram_out.stat().st_size
                    
                    # Tamanho incremental desta página
                    page_delta = actual_size - last_saved_size
                    if page_delta > 0:
                        page_sizes.append(page_delta)
                    last_saved_size = actual_size

                    # Verifica limite com tamanho REAL (não estimado)
                    exceeds_total = actual_size > max_bytes
                    exceeds_page  = page_delta > max_page_bytes and added > 1

                    if (exceeds_total or exceeds_page) and added > 1:
                        # Remove última página e fecha o volume atual
                        del part.pages[-1]
                        part.save(str(ram_out))
                        i -= 1
                        current_page -= 1
                        added -= 1
                        last_saved_size = ram_out.stat().st_size
                        break

            # === VERIFICAÇÃO PÓS-FECHAMENTO (antes do OCR) ===
            pre_ocr_size = ram_out.stat().st_size
            if pre_ocr_size > max_bytes:
                # Situação extrema: volume com 1 página já passa do limite
                # Loga o aviso mas mantém — não é possível dividir mais
                logging.warning(
                    f"Volume {vol}: página única excede o limite "
                    f"({pre_ocr_size / (1024*1024):.2f} MB > {max_mb} MB). "
                    f"Mantendo sem divisão adicional."
                )

            size_mb = pre_ocr_size / (1024 * 1024)
            logging.info(f"Volume {vol} criado com {added} páginas ({size_mb:.2f} MB)")
            if on_volume:
                on_volume(vol, added, size_mb)

            # === OCR SELETIVO: identifica páginas e aplica filtro de tamanho ===
            try:
                if on_ocr:
                    on_ocr(vol)

                # Identifica páginas que precisam OCR
                paginas_para_ocr = get_paginas_necessitam_ocr(str(ram_out))

                if paginas_para_ocr:
                    # Filtra apenas páginas seguras (tamanho < 500KB após OCR)
                    filtro_result = get_paginas_seguras_para_ocr(str(ram_out), paginas_para_ocr)
                    paginas_seguras = filtro_result["paginas_seguras"]

                    if paginas_seguras:
                        ocr(str(ram_out), str(ram_out), pages=paginas_seguras)
                        logging.info(
                            f"Volume {vol}: OCR seletivo aplicado em {len(paginas_seguras)} "
                            f"de {added} páginas (dentro do limite de 500KB)"
                        )
                    else:
                        logging.info(
                            f"Volume {vol}: Nenhuma página foi processada com OCR "
                            f"({len(paginas_para_ocr)} páginas candidatas excediam 500KB)"
                        )
                else:
                    logging.info(f"Volume {vol}: Nenhuma página candidata para OCR (sem imagens dominantes)")
            except Exception as e:
                raise RuntimeError(f"Falha no OCR seletivo do volume {vol}: {e}")

            # === VERIFICAÇÃO PÓS-OCR ===
            # O OCR pode aumentar o tamanho do arquivo significativamente
            post_ocr_size = ram_out.stat().st_size
            if post_ocr_size > max_bytes:
                logging.warning(
                    f"Volume {vol}: tamanho pós-OCR excede o limite "
                    f"({post_ocr_size / (1024*1024):.2f} MB). "
                    f"Considere reduzir o limite em ~10% para acomodar o overhead do OCR."
                )

            shutil.move(str(ram_out), str(final_out))
            vol += 1

    return {"cancelled": False, "total_pages": total}