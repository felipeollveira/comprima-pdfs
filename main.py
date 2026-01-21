from flask import Flask, render_template, request, send_file
import fitz  # PyMuPDF
from PIL import Image
import io
import os

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def processar_pdf_seletivo(input_path, output_path, paginas_para_comprimir, qualidade):
    doc_in = fitz.open(input_path)
    doc_out = fitz.open() # Novo PDF vazio

    # Normaliza páginas para comprimir
    paginas_para_comprimir = [int(p.strip()) for p in paginas_para_comprimir if p.strip().isdigit()]

    for i in range(len(doc_in)):
        doc_out.insert_pdf(doc_in, from_page=i, to_page=i)
        # Se a página está na lista, recomprime imagens
        if i in paginas_para_comprimir:
            page = doc_out[i]
            image_list = page.get_images(full=True)
            for img in image_list:
                xref = img[0]
                base_image = doc_out.extract_image(xref)
                image_bytes = base_image["image"]
                try:
                    img_pil = Image.open(io.BytesIO(image_bytes))
                    img_buffer = io.BytesIO()
                    img_pil.save(img_buffer, format="JPEG", quality=int(qualidade))
                    new_image_bytes = img_buffer.getvalue()
                    doc_out.update_image(xref, new_image_bytes)
                except Exception as e:
                    print(f"Erro ao recomprimir imagem na página {i}: {e}")

    # Salvando com compressão máxima
    doc_out.save(
        output_path,
        garbage=4,
        deflate=True,
        clean=True
    )
    doc_out.close()
    doc_in.close()

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        file = request.files['pdf']
        qualidade = request.form.get('qualidade', 50)
        # Lista de páginas que o usuário escolheu (ex: "1,2,5")
        paginas_selecionadas = request.form.get('paginas', "").split(',')
        
        input_path = os.path.join(UPLOAD_FOLDER, file.filename)
        output_path = os.path.join(UPLOAD_FOLDER, "otimizado_" + file.filename)
        file.save(input_path)

        processar_pdf_seletivo(input_path, output_path, paginas_selecionadas, qualidade)
        
        return send_file(output_path, as_attachment=True)

    return '''
    <!doctype html>
    <title>Compressor de PDF Inteligente</title>
    <h1>Upload e Compressão Seletiva</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=pdf> <br><br>
      Nível de Qualidade (1-100): <input type=number name=qualidade value=50> <br><br>
      Páginas para comprimir (ex: 0,1,2): <input type=text name=paginas> <br><br>
      <input type=submit value=Processar>
    </form>
    '''

if __name__ == '__main__':
    print("Iniciando o aplicativo de compressão de PDF...")
    app.run(debug=True)