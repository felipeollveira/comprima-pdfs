import os
import uuid
import json
import logging
import traceback
from flask import Flask, render_template, request, send_file, jsonify, Response, stream_with_context
import time
import threading

from engines.force_ocr import MAX_MB

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
                    # Chama split_volumes para dividir e OCRizar, salvando em uploads/ com padrão ocr_{task_id}_VOL_XX.pdf
                    from engines.force_ocr import split_volumes
                    import pathlib
                    split_volumes(
                        pathlib.Path(input_path),
                        pathlib.Path(UPLOAD_FOLDER),
                        MAX_MB
                    )
                    add_log("Páginas processadas com sucesso.")
                    # Verifica se existem arquivos divididos
                    from glob import glob
                    dividir_files = glob(os.path.join(UPLOAD_FOLDER, f"ocr_{task_id}_*.pdf"))
                    if dividir_files:
                        if len(dividir_files) > 1:
                            # Gera o ZIP e registra o nome
                            zip_name = f'documentos_divididos_{task_id}.zip'
                            progress_db[task_id]["final_file"] = zip_name
                            from io import BytesIO
                            import zipfile
                            zip_path = os.path.join(UPLOAD_FOLDER, zip_name)
                            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                                for f in dividir_files:
                                    arcname = os.path.basename(f).split('_', 2)[-1]
                                    zipf.write(f, arcname)
                            if os.path.exists(zip_path):
                                add_log(f"ZIP gerado com sucesso: {zip_path}")
                            else:
                                add_log(f"ERRO: ZIP NAO FOI GERADO: {zip_path}")
                        else:
                            # Só um volume, serve o PDF único
                            progress_db[task_id]["final_file"] = os.path.basename(dividir_files[0])
                            add_log(f"Só um volume gerado, servindo PDF: {dividir_files[0]}")
                    else:
                        output_filename = f"ocr_{task_id}.pdf"
                        progress_db[task_id]["final_file"] = output_filename
            else:
                output_filename = f"opt_{task_id}_{file.filename}"
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)

                def callback(page_index):
                    progress_db[task_id]["current"] = page_index + 1
                    progress_db[task_id]["status"] = f"Comprimindo página {page_index + 1}"
                    add_log(f"Página {page_index + 1} otimizada.")

                processar_pdf_custom(input_path, output_path, config_map, callback)
                progress_db[task_id]["final_file"] = output_filename

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
        "download_url": f"/download/{task_id}" # URL base
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
                 "final_file": task.get("final_file", "resultado.pdf")
            }
            # O SSE exige o formato 'data: <conteúdo>\n\n'
            yield f"data: {json.dumps(data)}\n\n"

            if task.get("status") in ["Concluído", "Falha no processamento"]: #
                break
            time.sleep(0.5)
            
    return Response(stream_with_context(event_stream()), mimetype='text/event-stream') #

@app.route('/download/<task_id>')
def download_file(task_id):
    """
    Rota dinâmica que serve o arquivo final (PDF ou ZIP) baseado no que foi
    processado na thread da tarefa específica.
    """
    task = progress_db.get(task_id)
    
    # Verifica se a tarefa existe e se já foi marcada como concluída
    if not task:
        return "Tarefa não encontrada.", 404
    
    if task.get("status") != "Concluído":
        return "O arquivo ainda está sendo processado. Por favor, aguarde.", 202

    # Recupera o nome do arquivo final que a thread salvou no progress_db
    filename = task.get("final_file")
    if not filename:
        return "Nome do arquivo não registrado no sistema.", 500

    file_path = os.path.join(UPLOAD_FOLDER, filename)

    # Verifica se o arquivo físico realmente existe no disco
    if os.path.exists(file_path):
        # Limpa o nome para o download (remove o UUID inicial)
        # Ex: 'uuid_original.pdf' -> 'original.pdf'
        clean_name = filename.split('_', 1)[-1] if '_' in filename else filename
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=clean_name,
            mimetype='application/octet-stream' # Garante compatibilidade com .zip e .pdf
        )
    
    return f"Erro: O arquivo {filename} não foi encontrado no servidor.", 404

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    # threaded=True permite que o polling ocorra enquanto o motor processa o PDF
    app.run(debug=True, port=5000, threaded=True)