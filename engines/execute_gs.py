import subprocess
import os
import fitz
import logging
from engines.locate_gs import localizar_gs

def executar_gs(input_path, output_path, nivel_idx):
    gs_exe = localizar_gs()
    perfis = {
        1: '/prepress', # 300 dpi
        2: '/printer',  # 300 dpi
        3: '/ebook',    # 150 dpi
        4: '/screen',   # 72 dpi
        5: '/screen'    # 42 dpi
    }
    perfil = perfis.get(int(nivel_idx), '/ebook')

    # Configurações base para otimização de tamanho
    cmd = [
        gs_exe, '-sDEVICE=pdfwrite', '-dCompatibilityLevel=1.4',
        f'-dPDFSETTINGS={perfil}',
        '-dNOPAUSE', '-dQUIET', '-dBATCH',
        '-dIgnorePageExtraStack',
        '-dAlwaysEmbed=false',              
        '-dEmbedAllFonts=false',
        '-dAutoRotatePages=/None', # Evita rotações inesperadas do GS
        '-dColorImageDownsampleType=/Bicubic',
        '-dGrayImageDownsampleType=/Bicubic',
        '-dMonoImageDownsampleType=/Bicubic'
    ]

    if int(nivel_idx) == 5:
        cmd.extend([
            '-dColorImageResolution=50',
            '-dGrayImageResolution=72',
            '-dMonoImageResolution=120',
            '-dDownsampleColorImages=true',
            '-dDownsampleGrayImages=true',
            '-dDownsampleMonoImages=true',
            '-dColorImageEncoder=/DCTEncode',
            '-dJPEGQ=60'
        ])
    else:
        res = '72' if int(nivel_idx) == 4 else '140'
        cmd.extend([
            f'-dColorImageResolution={res}',
            f'-dGrayImageResolution={res}',
            f'-dMonoImageResolution={res}'
        ])

    cmd.append(f'-sOutputFile={output_path}')
    cmd.append(input_path)
    
    subprocess.run(cmd, check=True, shell=(os.name == 'nt'))

def processar_pdf_custom(input_path, output_path, config_map, progress_callback=None):
    doc_in = fitz.open(input_path)
    temp_files = []
    try:
        from engines.force_ocr import ocr
        for i in range(len(doc_in)):
            t_in = f"temp_raw_{i}.pdf"
            t_ocr = f"temp_ocr_{i}.pdf"
            t_out = f"temp_comp_{i}.pdf"

            # 1. Extrair a página original e salvar em disco
            pag_doc = fitz.open()
            pag_doc.insert_pdf(doc_in, from_page=i, to_page=i)
            pag_doc.save(t_in)
            pag_doc.close()

            # 2. Sempre faz OCR na página, independentemente do tipo de compressão
            try:
                ocr(t_in, t_ocr)
                ocr_input = t_ocr
                if os.path.exists(t_in): os.remove(t_in)
            except Exception as e:
                print(f"Erro ao rodar OCR na página {i}: {e}")
                ocr_input = t_in  # fallback: usa original

            # 3. Definir o nível e comprimir o arquivo OCR
            nivel = config_map.get(str(i), 3)
            try:
                executar_gs(ocr_input, t_out, nivel)
                temp_files.append(t_out)
                if os.path.exists(ocr_input) and ocr_input != t_in:
                    os.remove(ocr_input)
            except Exception as e:
                print(f"Erro ao comprimir página {i}: {e}")
                temp_files.append(ocr_input)

            # 4. Notificar o progresso para o app.py
            if progress_callback:
                progress_callback(i)
            for f in [t_in, t_ocr]:
                if os.path.exists(f) and f != temp_files[-1]:
                    os.remove(f)

            if progress_callback:
                progress_callback(i)

        # 5. União dos arquivos processados
        doc_out = fitz.open()
        for f in temp_files:
            if os.path.exists(f):
                src_page = fitz.open(f)
                doc_out.insert_pdf(src_page)
                src_page.close()
                os.remove(f) # Remove o temp após inserir no final
        
        doc_out.save(output_path, garbage=4, deflate=True)
        doc_out.close()

    finally:
        doc_in.close()
        # Garantia final: limpa qualquer arquivo temp_ residual desse processo
        for f in os.listdir('.'):
            if f.startswith(f"temp_{os.getpid()}_"):
                try: os.remove(f)
                except: pass