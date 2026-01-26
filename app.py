import os
import uuid
import json
import logging
import traceback
from flask import Flask, render_template, request, send_file, jsonify
from PyPDF2 import PdfReader

from engines.execute_gs import processar_pdf_custom

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.static_folder = 'templates'

# Configuração de Log básica
logging.basicConfig(level=logging.INFO)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Armazena o progresso: { task_id: { "current": 0, "total": 100, "status": "...", "logs": "" } }
progress_db = {}

@app.route('/processar', methods=['POST'])
def processar():
    if 'pdf' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
        
    file = request.files['pdf']
    config_map = json.loads(request.form.get('config_map', '{}'))
    task_id = str(uuid.uuid4())
    
    # Define caminhos e salva arquivo
    input_filename = f"{task_id}_{file.filename}"
    input_path = os.path.join(UPLOAD_FOLDER, input_filename)
    file.save(input_path)

    # Inicializa estrutura de dados da Task
    progress_db[task_id] = {
        "current": 0, 
        "total": len(config_map) if config_map else 1, 
        "status": "Iniciando conexão...", 
        "logs": f"[{task_id[-6:]}] Arquivo recebido: {file.filename}\n"
    }

    def add_log(msg):
        progress_db[task_id]["logs"] += f"[{task_id[-6:]}] {msg}\n"
        logging.info(msg)

    # Identifica se há páginas marcadas para OCR (Nível 6)
    ocr_pages = [int(idx) + 1 for idx, v in config_map.items() if int(v) == 6]
    
    try:
        if ocr_pages:
            from engines.force_ocr import ocr
            ocr_filename = f"ocr_{task_id}.pdf"
            ocr_path = os.path.join(UPLOAD_FOLDER, ocr_filename)
            
            reader = PdfReader(input_path)
            total_pages = len(reader.pages)
            
            add_log(f"Iniciando motor OCR para {len(ocr_pages)} páginas.")
            progress_db[task_id].update({"total": 1, "status": "Executando OCR e Otimização JBIG2..."})

            if len(ocr_pages) == total_pages:
                ocr(input_path, ocr_path)
            else:
                pages_str = ",".join(str(p) for p in sorted(ocr_pages))
                ocr(input_path, ocr_path, pages=pages_str)

            add_log("OCR concluído com sucesso.")
            progress_db[task_id].update({"current": 1, "status": "Concluído"})
            return jsonify({"task_id": task_id, "download_url": f"/download/{task_id}/{ocr_filename}"})

        else:
            output_filename = f"opt_{task_id}_{file.filename}"
            output_path = os.path.join(UPLOAD_FOLDER, output_filename)

            def update_progress(page_index):
                msg = f"Página {page_index + 1} de {len(config_map)} processada."
                progress_db[task_id]["current"] = page_index + 1
                progress_db[task_id]["status"] = f"Otimizando: {page_index + 1}/{len(config_map)}"
                add_log(msg)

            add_log(f"Iniciando compressão Ghostscript: {len(config_map)} páginas.")
            processar_pdf_custom(input_path, output_path, config_map, update_progress)
            
            add_log("Otimização concluída.")
            progress_db[task_id]["status"] = "Concluído"
            return jsonify({"task_id": task_id, "download_url": f"/download/{task_id}/{output_filename}"})

    except Exception as e:
        error_trace = traceback.format_exc()
        add_log(f"ERRO: {str(e)}")
        logging.error(error_trace)
        progress_db[task_id]["status"] = "Falha no processamento"
        return jsonify({"error": str(e), "logs": error_trace}), 500

@app.route('/status/<task_id>')
def status(task_id):
    return jsonify(progress_db.get(task_id, {
        "status": "Não encontrado", 
        "current": 0, "total": 1, 
        "logs": "ID de tarefa inválido.\n"
    }))

@app.route('/download/<task_id>/<filename>')
def download(task_id, filename):
    path = os.path.join(UPLOAD_FOLDER, filename)
    
    if not os.path.exists(path):
        # Tenta localizar por padrões conhecidos se o link direto falhar
        possibilidades = [
            os.path.join(UPLOAD_FOLDER, f"opt_{task_id}_{filename}"),
            os.path.join(UPLOAD_FOLDER, f"ocr_{task_id}.pdf")
        ]
        for p in possibilidades:
            if os.path.exists(p):
                path = p
                break

    if os.path.exists(path):
        # Nome limpo para o usuário (remove o UUID)
        clean_name = filename.split('_', 1)[-1] if '_' in filename else filename
        if "ocr_" in filename: clean_name = "documento_pesquisavel.pdf"
        
        return send_file(path, as_attachment=True, download_name=clean_name)
            
    return "Arquivo não encontrado.", 404

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    # threaded=True permite que o polling ocorra enquanto o motor processa o PDF
    app.run(debug=True, port=5000, threaded=True)