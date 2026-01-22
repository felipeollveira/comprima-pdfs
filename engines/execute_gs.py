import subprocess
import os
import fitz
from engines.locate_gs import localizar_gs

def executar_gs(input_path, output_path, nivel_idx):
    gs_exe = localizar_gs()
    perfis = {
        1: '/prepress', # 300 dpi
        2: '/printer',  # 300 dpi
        3: '/ebook',    # 150 dpi
        4: '/screen',    # 72 dpi
        5: '/screen'    # 42 dpi (usaremos screen com ajuste manual depois)
    }
    perfil = perfis.get(int(nivel_idx), '/ebook')

    if nivel_idx == 5:
        # Para "Muito Alta", ajustamos a resolução manualmente
        cmd = [
        gs_exe, '-sDEVICE=pdfwrite', '-dCompatibilityLevel=1.4',
        f'-dPDFSETTINGS={perfil}',
        '-dAlwaysEmbed=false',              
        '-dEmbedAllFonts=false',         
        '-dColorImageDownsampleType=/Bicubic',
        '-dColorImageResolution=50',
        '-dNOPAUSE', '-dQUIET', '-dBATCH',
        '-dGrayImageResolution=72',
        '-dMonoImageResolution=150',
        '-dDownsampleColorImages=true',
        '-dDownsampleGrayImages=true',
        '-dDownsampleMonoImages=true',
        '-dGrayImageDownsampleType=/Bicubic',
        '-dAutoFilterColorImages=false',
        '-dColorImageEncoder=/DCTEncode',
        '-dJPEGQ=60',
        f'-sOutputFile={output_path}', input_path
    ]   
        
    else:
        cmd = [
            gs_exe, '-sDEVICE=pdfwrite', '-dCompatibilityLevel=1.4',
            f'-dPDFSETTINGS={perfil}',
            '-dAlwaysEmbed=false',              # Não forçar embutir todas as fontes
            '-dEmbedAllFonts=false',            # Tentar remover fontes desnecessárias
            '-dColorImageDownsampleType=/Bicubic', # Melhor algoritmo de compressão
            '-dColorImageResolution=72' if nivel_idx == 4 else '-dColorImageResolution=140',
            '-dNOPAUSE', '-dQUIET', '-dBATCH',
            f'-sOutputFile={output_path}', input_path
        ]
    subprocess.run(cmd, check=True, shell=(os.name == 'nt'))

def processar_pdf_custom(input_path, output_path, config_map, progress_callback=None):
    doc_in = fitz.open(input_path)
    temp_files = []
    
    try:
        for i in range(len(doc_in)):
            t_in = f"temp_raw_{i}.pdf"
            t_out = f"temp_comp_{i}.pdf"
            
            # 1. PRIMEIRO: Extrair a página original e salvar em disco
            pag_doc = fitz.open()
            pag_doc.insert_pdf(doc_in, from_page=i, to_page=i)
            pag_doc.save(t_in)
            pag_doc.close()

            # 2. SEGUNDO: Definir o nível e comprimir o arquivo recém-criado
            nivel = config_map.get(str(i), 3)
            try:
                executar_gs(t_in, t_out, nivel)
                temp_files.append(t_out)
                # Remove o temporário original (raw) após compressão com sucesso
                if os.path.exists(t_in): os.remove(t_in)
            except Exception as e:
                print(f"Erro ao comprimir página {i}: {e}")
                # Se o GS falhar, usamos a página original (raw) como fallback
                temp_files.append(t_in)

            # 3. TERCEIRO: Notificar o progresso para o app.py
            if progress_callback:
                progress_callback(i)

        # 4. Reunião final de todos os pedaços
        doc_out = fitz.open()
        for f in temp_files:
            if os.path.exists(f):
                src_page = fitz.open(f)
                doc_out.insert_pdf(src_page)
                src_page.close()
                os.remove(f) # Limpa o pedaço após unir

        doc_out.save(output_path, garbage=4, deflate=True)
        doc_out.close()
        
    finally:
        doc_in.close()