import os
import uuid
import json
from flask import Flask, render_template, request, send_file, jsonify
from pdf_engine import processar_pdf_custom

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

from flask import Flask, render_template, request, send_file, jsonify
import json, os, uuid
from pdf_engine import processar_pdf_custom

app = Flask(__name__)
# Armazena o progresso: { task_id: { "current": 0, "total": 100 } }
progress_db = {}

@app.route('/processar', methods=['POST'])
def processar():
    file = request.files['pdf']
    config_map = json.loads(request.form.get('config_map', '{}'))
    task_id = str(uuid.uuid4())
    
    input_path = os.path.join('uploads', f"{task_id}_{file.filename}")
    output_path = os.path.join('uploads', f"opt_{task_id}_{file.filename}")
    file.save(input_path)

    # Inicializa progresso
    progress_db[task_id] = {"current": 0, "total": len(config_map), "status": "Iniciando..."}

    # Função de callback para atualizar o progresso
    def update_progress(page_index):
        progress_db[task_id]["current"] = page_index + 1
        progress_db[task_id]["status"] = f"Comprimindo página {page_index + 1}..."

    # Chamamos o processamento (idealmente em uma Thread, mas para teste faremos sequencial)
    try:
        processar_pdf_custom(input_path, output_path, config_map, update_progress)
        progress_db[task_id]["status"] = "Concluído"
        return jsonify({"task_id": task_id, "download_url": f"/download/{task_id}/{file.filename}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/status/<task_id>')
def status(task_id):
    return jsonify(progress_db.get(task_id, {"status": "Não encontrado"}))

@app.route('/download/<task_id>/<filename>')
def download(task_id, filename):
    path = os.path.join('uploads', f"opt_{task_id}_{filename}")
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
    app.run(debug=True)