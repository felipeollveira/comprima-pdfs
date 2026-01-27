import os
import uuid
import json
import logging
import traceback
from flask import Flask, render_template, request, send_file, jsonify, Response, stream_with_context
import time
import threading

app = Flask(__name__)

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
    
    input_filename = f"{task_id}_{file.filename}"
    input_path = os.path.join(UPLOAD_FOLDER, input_filename)
    file.save(input_path)

    # Inicializa o banco de dados de progresso
    progress_db[task_id] = {
        "current": 0, 
        "total": len(config_map) if config_map else 1, 
        "status": "Iniciando...", 
        "logs": f"[{time.strftime('%H:%M:%S')}] Arquivo '{file.filename}' recebido.\n"
    }

    def worker_process():
        """Função que roda em segundo plano para não travar o SSE"""
        def add_log(msg):
            progress_db[task_id]["logs"] += f"[{time.strftime('%H:%M:%S')}] {msg}\n"

        try:
            # Lógica de OCR ou Compressão Customizada
            is_ocr = any(int(v) == 6 for v in config_map.values())
            
            if is_ocr:
                add_log("Iniciando motor de OCR...")
                progress_db[task_id]["status"] = "Executando OCR..."
                # Sua chamada de OCR aqui...
                time.sleep(2) # Simulação
                add_log("Páginas processadas com sucesso.")
                output_filename = f"ocr_{task_id}.pdf"
                progress_db[task_id]["filename"] = output_filename
            else:
                output_filename = f"opt_{task_id}_{file.filename}"
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)

                def callback(page_index):
                    progress_db[task_id]["current"] = page_index + 1
                    progress_db[task_id]["status"] = f"Comprimindo página {page_index + 1}"
                    add_log(f"Página {page_index + 1} otimizada.")

                processar_pdf_custom(input_path, output_path, config_map, callback)
                progress_db[task_id]["filename"] = output_filename

            add_log("Processo finalizado com sucesso.")
            progress_db[task_id]["status"] = "Concluído"

        except Exception as e:
            add_log(f"ERRO CRÍTICO: {str(e)}")
            progress_db[task_id]["status"] = "Falha no processamento"

    # LANÇA A THREAD E LIBERA O FLASK PARA O SSE
    thread = threading.Thread(target=worker_process)
    thread.start()

    return jsonify({
        "task_id": task_id,
        "download_url": f"/download/{task_id}/resultado.pdf" # URL base
    })


@app.route('/progress/<task_id>')
def progress(task_id):
    def event_stream():
        last_log_len = 0
        while True:
            task = progress_db.get(task_id) #
            if not task:
                break
            
            logs = task.get("logs", "") #
            new_logs = []
            if len(logs) > last_log_len:
                # Extrai apenas as linhas novas
                new_content = logs[last_log_len:]
                new_logs = [line for line in new_content.splitlines() if line.strip()]
                last_log_len = len(logs)

            # Criamos um payload com progresso e logs
            current = task.get("current", 0)
            total = task.get("total", 1)
            data = {
                "percent": (current / total) * 100,
                "status": task.get("status"),
                "logs": new_logs,
                "filename": task.get("filename", "resultado.pdf")
            }
            # O SSE exige o formato 'data: <conteúdo>\n\n'
            yield f"data: {json.dumps(data)}\n\n"

            if task.get("status") in ["Concluído", "Falha no processamento"]: #
                break
            time.sleep(0.5)
            
    return Response(stream_with_context(event_stream()), mimetype='text/event-stream') #

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