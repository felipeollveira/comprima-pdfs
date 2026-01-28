import os
import uuid
import json
import logging
import time
import threading
import glob
import zipfile
from flask import Flask, render_template, request, send_file, jsonify, Response, stream_with_context
import pathlib

# Importações dos motores internos
from engines.force_ocr import split_volumes, MAX_MB
from engines.execute_gs import processar_pdf_custom

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.static_folder = 'templates'

# Configuração de Log
logging.basicConfig(level=logging.INFO)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Banco de dados temporário de progresso
progress_db = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/processar', methods=['POST'])
def processar():
    if 'pdf' not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400
        
    file = request.files['pdf']
    config_map = json.loads(request.form.get('config_map', '{}'))
    task_id = str(uuid.uuid4())
    
    # O nome do arquivo original no servidor terá o task_id como prefixo
    input_filename = f"{task_id}_{file.filename}"
    input_path = os.path.join(UPLOAD_FOLDER, input_filename)
    file.save(input_path)

    progress_db[task_id] = {
        "current": 0, 
        "total": len(config_map) if config_map else 1, 
        "status": "Iniciando...", 
        "logs": f"[{time.strftime('%H:%M:%S')}] Arquivo recebido.\n",
        "final_file": None
    }

    def worker_process():
        def add_log(msg):
            progress_db[task_id]["logs"] += f"[{time.strftime('%H:%M:%S')}] {msg}\n"

        try:
            # Verifica se alguma página solicitou OCR (valor 6)
            is_ocr = any(int(v) == 6 for v in config_map.values())
            
            if is_ocr:
                add_log("Iniciando motor de OCR e Divisão de Volumes...")
                progress_db[task_id]["status"] = "Executando OCR..."
                
                # split_volumes gera arquivos seguindo o padrão: {task_id}_{filename}_VOL_XX.pdf
                split_volumes(
                    pathlib.Path(input_path),
                    pathlib.Path(UPLOAD_FOLDER),
                    MAX_MB
                )
                
                # CORREÇÃO DO GLOB: Busca arquivos que começam com o ID da tarefa e contêm _VOL_
                pattern = os.path.join(UPLOAD_FOLDER, f"{task_id}_*_VOL_*.pdf")
                dividir_files = sorted(glob.glob(pattern))
                
                if dividir_files:
                    if len(dividir_files) > 1:
                        zip_name = f'volumes_{task_id}.zip'
                        zip_path = os.path.join(UPLOAD_FOLDER, zip_name)
                        
                        add_log(f"Compactando {len(dividir_files)} volumes em ZIP...")
                        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for f in dividir_files:
                                # Nome limpo dentro do ZIP (remove o UUID do início)
                                arcname = os.path.basename(f).split('_', 1)[-1]
                                zipf.write(f, arcname)
                        
                        progress_db[task_id]["final_file"] = zip_name
                    else:
                        # Se gerou apenas 1 arquivo, serve o PDF diretamente
                        progress_db[task_id]["final_file"] = os.path.basename(dividir_files[0])
                else:
                    raise Exception("Nenhum arquivo de volume foi gerado pelo motor de OCR.")

            else:
                # Fluxo de Otimização Normal
                output_filename = f"opt_{task_id}_{file.filename}"
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)

                def callback(page_index):
                    progress_db[task_id]["current"] = page_index + 1
                    progress_db[task_id]["status"] = f"Otimizando página {page_index + 1}"
                    add_log(f"Página {page_index + 1} processada.")

                processar_pdf_custom(input_path, output_path, config_map, callback)
                progress_db[task_id]["final_file"] = output_filename

            add_log("Processo concluído com sucesso.")
            progress_db[task_id]["status"] = "Concluído"

        except Exception as e:
            add_log(f"ERRO: {str(e)}")
            progress_db[task_id]["status"] = "Falha no processamento"

    threading.Thread(target=worker_process).start()
    return jsonify({"task_id": task_id})

@app.route('/progress/<task_id>')
def progress(task_id):
    def event_stream():
        last_log_len = 0
        while True:
            task = progress_db.get(task_id)
            if not task: break
            
            logs = task.get("logs", "")
            new_logs = []
            if len(logs) > last_log_len:
                new_logs = [l for l in logs[last_log_len:].splitlines() if l.strip()]
                last_log_len = len(logs)

            data = {
                "percent": (task["current"] / task["total"]) * 100 if task["total"] > 0 else 0,
                "status": task["status"],
                "logs": new_logs,
                "final_file": task.get("final_file")
            }
            yield f"data: {json.dumps(data)}\n\n"

            if task["status"] in ["Concluído", "Falha no processamento"]:
                break
            time.sleep(0.5)
            
    return Response(stream_with_context(event_stream()), mimetype='text/event-stream')

@app.route('/download/<task_id>')
def download_file(task_id):
    task = progress_db.get(task_id)
    if not task or task.get("status") != "Concluído":
        return "Arquivo não disponível.", 404

    filename = task.get("final_file")
    file_path = os.path.join(UPLOAD_FOLDER, filename)

    if os.path.exists(file_path):
        # Remove o UUID do nome para o usuário final
        clean_name = filename.replace(f"_{task_id}", "").replace(f"{task_id}_", "")
        return send_file(file_path, as_attachment=True, download_name=clean_name)
    
    return "Erro: Arquivo físico não encontrado.", 404

if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)