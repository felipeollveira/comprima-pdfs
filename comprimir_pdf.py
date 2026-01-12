import subprocess
import os
import fitz  # PyMuPDF

def analisar_estrategia_ia(input_path):
    """
    Simula uma IA de decisão: analisa a composição do PDF para sugerir o nível de compressão.
    Retorna um valor de 1 a 4.
    """
    try:
        doc = fitz.open(input_path)
        total_paginas = len(doc)
        total_imagens = 0
        area_total_imagens = 0

        for pagina in doc:
            images = pagina.get_images(full=True)
            total_imagens += len(images)
            
            # Analisa o tamanho das imagens para ver se são alta res
            for img in images:
                xref = img[0]
                pix = doc.extract_image(xref)
                area_total_imagens += pix["width"] * pix["height"]

        doc.close()

        # Lógica de Decisão (Heurística):
        # 1. Se não tem imagens, compressão leve (apenas limpeza de metadados)
        if total_imagens == 0:
            return 1 
        
        # 2. Calcula densidade (Imagens por página)
        densidade = total_imagens / total_paginas
        # 3. Média de "peso" das imagens (em megapixels aproximados)
        peso_medio = (area_total_imagens / total_imagens) / 1_000_000

        print(f"Análise: {total_imagens} imagens encontradas. Peso médio: {peso_medio:.2f}MP.")

        if peso_medio > 2.0 or densidade > 5:
            return 4  # Foto-pesado ou Scans: Compressão Máxima
        elif peso_medio > 0.5:
            return 3  # Documento comum com fotos: Compressão Média
        else:
            return 2  # Majoritariamente texto: Boa qualidade
            
    except Exception as e:
        print(f"Falha na análise, usando padrão: {e}")
        return 3

def comprimir_com_ghostscript(input_path, output_path, power=None):
    # Se o power não for passado, a IA decide
    if power is None:
        print("Analisando melhor estratégia de compressão...")
        power = analisar_estrategia_ia(input_path)
        print(f"Estratégia selecionada: Nível {power}")

    quality = {
        0: '/default',
        1: '/prepress',
        2: '/printer',
        3: '/ebook',
        4: '/screen'
    }

    pdf_settings = quality.get(power)
    gs_executable = "gs" if os.name != "nt" else "gswin64c"
    
    arg_list = [
        gs_executable,
        '-sDEVICE=pdfwrite',
        '-dCompatibilityLevel=1.4',
        f'-dPDFSETTINGS={pdf_settings}', 
        '-dNOPAUSE', '-dQUIET', '-dBATCH',
        '-dEmbedAllFonts=true', 
        '-dSubsetFonts=true',
        '-dColorImageDownsampleType=/Bicubic',
        '-dColorImageResolution=150' if power <= 3 else '-dColorImageResolution=72',
        f'-sOutputFile={output_path}',
        input_path
    ]
    
    if not os.path.exists(input_path):
        print(f"Erro: Arquivo não encontrado: {input_path}")
        return

    tamanho_inicial = os.path.getsize(input_path) / (1024 * 1024)
    print(f"Tamanho inicial: {tamanho_inicial:.2f} MB")

    try:
        subprocess.run(arg_list, check=True)
        tamanho_final = os.path.getsize(output_path) / (1024 * 1024)
        reducao = ((tamanho_inicial - tamanho_final) / tamanho_inicial) * 100
        print(f"Tamanho final: {tamanho_final:.2f} MB | Redução: {reducao:.1f}%")
        
    except subprocess.CalledProcessError as e:
        print(f"Erro no Ghostscript: {e}")

if __name__ == "__main__":
    entrada = r"C:\Users\foliveira\Downloads\Solicitacao de aproveitamento de materias.pdf"
    saida = r"C:\Users\foliveira\Desktop\Work\Space\pdf\comprimir-pdf\example-final.pdf"

    comprimir_com_ghostscript(entrada, saida)