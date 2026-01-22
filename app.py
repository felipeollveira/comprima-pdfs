import os
import pathlib
import shutil
import uuid
import json
from flask import Flask, render_template, request, send_file, jsonify

from engines.execute_gs import processar_pdf_custom



app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Armazena o progresso: { task_id: { "current": 0, "total": 100 } }
progress_db = {}
@app.route('/processar', methods=['POST'])
def processar():
    if 'pdf' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
        
    file = request.files['pdf']
    config_map = json.loads(request.form.get('config_map', '{}'))
    task_id = str(uuid.uuid4())
    
    # Define caminhos
    input_path = os.path.join(UPLOAD_FOLDER, f"{task_id}_{file.filename}")
    file.save(input_path) # Salva uma única vez aqui

    is_ocr_task = any(v == 6 for v in config_map.values())

    if is_ocr_task:
        progress_db[task_id] = {"current": 1, "total": 4, "status": "Iniciando OCR (Processo Lento)..."}
        try:
            from engines.force_ocr import ocr, split_volumes
            
            ocr_path = os.path.join(UPLOAD_FOLDER, f"ocr_{task_id}.pdf")
            ocr(input_path, ocr_path)
            
            progress_db[task_id] = {"current": 2, "total": 4, "status": "Dividindo em volumes..."}
            out_dir = pathlib.Path(UPLOAD_FOLDER) / task_id
            split_volumes(pathlib.Path(ocr_path), out_dir, 5.0)
            
            progress_db[task_id] = {"current": 3, "total": 4, "status": "Criando pacote ZIP..."}
            zip_name = f"volumes_{task_id}"
            # O shutil.make_archive adiciona o .zip automaticamente
            shutil.make_archive(os.path.join(UPLOAD_FOLDER, zip_name), 'zip', out_dir)
            
            progress_db[task_id] = {"current": 4, "total": 4, "status": "Concluído"}
            return jsonify({"task_id": task_id, "download_url": f"/download_zip/{zip_name}.zip"})
            
        except Exception as e:
            return jsonify({"error": f"Erro no OCR: {str(e)}"}), 500
    else:
        output_path = os.path.join(UPLOAD_FOLDER, f"opt_{task_id}_{file.filename}")
        progress_db[task_id] = {"current": 0, "total": len(config_map), "status": "Iniciando..."}

        def update_progress(page_index):
            progress_db[task_id]["current"] = page_index + 1
            progress_db[task_id]["status"] = f"Comprimindo página {page_index + 1}..."

        try:
            processar_pdf_custom(input_path, output_path, config_map, update_progress)
            progress_db[task_id]["status"] = "Concluído"
            return jsonify({"task_id": task_id, "download_url": f"/download/{task_id}/{file.filename}"})
        except Exception as e:
            return jsonify({"error": f"Erro na compressão: {str(e)}"}), 500



@app.route('/status/<task_id>')
def status(task_id):
    return jsonify(progress_db.get(task_id, {"status": "Não encontrado"}))

@app.route('/download/<task_id>/<filename>')
def download(task_id, filename):
    # Busca o arquivo otimizado pelo nome correto
    opt_name = f"opt_{task_id}_{filename}"
    path = os.path.join(UPLOAD_FOLDER, opt_name)
    if not os.path.exists(path):
        return "Arquivo não encontrado", 404
    return send_file(path, as_attachment=True)

# Endpoint para download do zip OCR+Dividir
@app.route('/download_zip/<zipname>')
def download_zip(zipname):
    path = os.path.join(UPLOAD_FOLDER, zipname)
    if not os.path.exists(path):
        return "Arquivo ZIP não encontrado", 404
    return send_file(path, as_attachment=True)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        file = request.files['pdf']
        # O mapa de páginas vem como uma string JSON do campo hidden
        config_map_raw = request.form.get('config_map', '{}')
        config_map = json.loads(config_map_raw)
        
        input_path = os.path.join(UPLOAD_FOLDER, file.filename)
        output_path = os.path.join(UPLOAD_FOLDER, "otimizado_" + file.filename)
        file.save(input_path)

        processar_pdf_custom(input_path, output_path, config_map)
        return send_file(output_path, as_attachment=True)

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)