import os
import uuid
import json
import logging
import time
import threading
import glob
import zipfile
from datetime import datetime
import pathlib

os.environ["OMP_THREAD_LIMIT"] = "1"

from flask import (
    Flask,
    render_template,
    request,
    send_file,
    jsonify,
    Response,
    stream_with_context,
    send_from_directory,
)

from analytics import log_uso, log_feedback, compute_metrics, read_tail_uso, read_tail_feedback
from engines.force_ocr import split_volumes, MAX_MB
from engines.execute_gs import processar_pdf_custom
from engines.signature import has_signature
from engines.ramdisk import temp_dir, cleanup_temp_dir
from engines.high_performance_ocr import process_pdf_high_performance
from engines.split_only import split_pdf_only


try:
    from zoneinfo import ZoneInfo
    SAO_PAULO_TZ = ZoneInfo("America/Sao_Paulo")
except ImportError:
    import pytz
    SAO_PAULO_TZ = pytz.timezone("America/Sao_Paulo")


def sao_paulo_time_str():
    return datetime.now(SAO_PAULO_TZ).strftime("%H:%M:%S")


def get_user():
    return ""


# ✅ Mantendo templates/ como pasta de template
app = Flask(__name__, template_folder="templates", static_folder="static")

UPLOAD_FOLDER = "uploads"
logging.basicConfig(level=logging.INFO)
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

progress_db = {}


def _mode(values):
    return max(set(values), key=values.count)


def _resolve_hp_level_from_config(config_map):
    """
    Resolve nível de compressão para o HP-OCR com base no que veio do front.

    Prioridade:
      1) DPI direto (20..600), ex.: 150, 70
      2) Presets 1..5 (compressão padrão do front)
      3) Fallback 3 (média / 150dpi)
    """
    if not config_map:
        return 3

    values = []
    for v in config_map.values():
        try:
            values.append(int(v))
        except (TypeError, ValueError):
            continue

    if not values:
        return 3

    dpi_values = [v for v in values if 20 <= v <= 600]
    if dpi_values:
        return _mode(dpi_values)

    preset_values = [v for v in values if v in (1, 2, 3, 4, 5)]
    if preset_values:
        return _mode(preset_values)

    return 3


# ✅ SERVIR CSS/JS de dentro de templates/
# Seus arquivos ficam em:
# templates/css/style.css  -> /assets/css/style.css
# templates/js/scripts.js  -> /assets/js/scripts.js
@app.route("/assets/<path:filename>")
def assets(filename):
    base = os.path.join(app.root_path, "templates")
    return send_from_directory(base, filename)


@app.route("/verificar-assinatura", methods=["POST"])
def verificar_assinatura():
    if "pdf" not in request.files:
        return jsonify({"assinatura": False, "error": "Nenhum arquivo enviado"}), 400

    file = request.files["pdf"]
    assinatura = False

    try:
        import uuid as _uuid
        tmp_path = os.path.join(temp_dir(), f"sig_{_uuid.uuid4().hex}.pdf")
        try:
            file.save(tmp_path)
            assinatura = has_signature(tmp_path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
    except Exception as e:
        logging.warning(f"Falha ao verificar assinatura digital: {e}")
        return jsonify({"assinatura": False, "error": str(e)}), 500

    return jsonify({"assinatura": assinatura})


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/processar", methods=["POST"])
def processar():
    cliente_ip = request.remote_addr or ""
    usuario = get_user()
    endpoint = request.path

    if "pdf" not in request.files:
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    file = request.files["pdf"]

    # ===== Log: Upload =====
    log_uso(
        acao="upload",
        modulo="processar",
        ip=cliente_ip,
        usuario=usuario,
        descricao="Arquivo enviado para processamento",
        observacao=f"endpoint={endpoint}",
    )

    # Validação de tamanho (limite: 499MB)
    MAX_FILE_SIZE_MB = 499

    file_size_bytes = request.content_length
    if file_size_bytes is None:
        raw = file.read()
        file_size_bytes = len(raw)
        file.seek(0)

    file_size_mb = file_size_bytes / (1024 * 1024)

    if file_size_mb > MAX_FILE_SIZE_MB:
        log_uso(
            acao="erro",
            modulo="processar",
            ip=cliente_ip,
            usuario=usuario,
            descricao="Arquivo muito grande",
            observacao=f"tamanho_mb={file_size_mb:.1f};endpoint={endpoint}",
        )
        return jsonify({"error": f"Arquivo muito grande ({file_size_mb:.1f}MB). Máximo: {MAX_FILE_SIZE_MB}MB"}), 413

    config_map = json.loads(request.form.get("config_map", "{}"))
    task_id = str(uuid.uuid4())

    input_filename = f"{task_id}_{file.filename}"
    input_path = os.path.join(UPLOAD_FOLDER, input_filename)
    file.save(input_path)

    assinatura = False
    try:
        assinatura = has_signature(input_path)
    except Exception as e:
        logging.warning(f"Falha ao verificar assinatura digital: {e}")

    progress_db[task_id] = {
        "current": 0,
        "total": len(config_map) if config_map else 1,
        "status": "Iniciando...",
        "logs": f"[{sao_paulo_time_str()}] Arquivo recebido ({file_size_mb:.2f}MB).\n",
        "final_file": None,
        "assinatura": assinatura,
        "cancelled": False,
        "start_time": time.time(),
    }

    def worker_process():
        def add_log(msg):
            progress_db[task_id]["logs"] += f"[{sao_paulo_time_str()}] {msg}\n"

        start_ts = time.time()

        log_uso(
            acao="início",
            modulo="processar",
            ip=cliente_ip,
            usuario=usuario,
            descricao="Processamento iniciado",
            observacao=f"id_tarefa={task_id}",
        )

        try:
            # 🚀 Motor OCR com Alta Performance + Divisão posterior (níveis 6 e 7)
            is_ocr = any(int(v) in [6, 7] for v in (config_map or {}).values())

            if is_ocr:
                add_log("Iniciando OCR de Alta Performance no PDF completo...")
                progress_db[task_id]["status"] = "OCR Alta Performance em andamento..."
                progress_db[task_id]["current"] = 0
                progress_db[task_id]["total"] = 0

                hp_level = _resolve_hp_level_from_config(config_map)
                add_log(f"HP-OCR usando nível de compressão: {hp_level}")

                output_filename = f"opt_{task_id}_{file.filename}"
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)

                def hp_callback(current_page, total_pages):
                    progress_db[task_id]["current"] = current_page
                    progress_db[task_id]["total"] = total_pages
                    progress_db[task_id]["status"] = (
                        f"OCR: {current_page}/{total_pages} páginas"
                    )
                    if current_page % 5 == 0 or current_page == total_pages:
                        add_log(f"OCR: {current_page}/{total_pages} páginas processadas.")

                ocr_success = False
                try:
                    # 🚀 1ª tentativa: HP-OCR no PDF completo
                    result_path = process_pdf_high_performance(
                        input_path,
                        callback=hp_callback,
                        compression_level=hp_level,
                    )

                    # Move o resultado para o caminho esperado
                    if os.path.abspath(result_path) != os.path.abspath(output_path):
                        import shutil as _shutil
                        _shutil.move(result_path, output_path)

                    add_log("Motor HP-OCR concluído com sucesso.")
                    ocr_success = True

                except Exception as hp_error:
                    add_log(f"HP-OCR falhou: {hp_error}. Tentando OCR tradicional...")
                    logging.warning(f"Fallback para OCR tradicional: {hp_error}")
                    
                    # 📄 Fallback: OCR tradicional no PDF completo
                    try:
                        progress_db[task_id]["status"] = "OCR tradicional em andamento..."
                        
                        def callback(processed_count, total_pages):
                            progress_db[task_id]["current"] = processed_count + 1
                            progress_db[task_id]["total"] = total_pages
                            progress_db[task_id]["status"] = f"OCR tradicional: {processed_count + 1}/{total_pages} páginas"
                            add_log(f"OCR tradicional: {processed_count + 1}/{total_pages} páginas.")

                        def check_cancelled():
                            return progress_db[task_id]["cancelled"]

                        processar_pdf_custom(input_path, output_path, config_map, callback, check_cancelled)
                        add_log("OCR tradicional concluído com sucesso.")
                        ocr_success = True
                        
                    except Exception as trad_error:
                        add_log(f"OCR tradicional também falhou: {trad_error}")
                        raise Exception(f"Falha em ambos os motores de OCR: HP-OCR ({hp_error}) e Tradicional ({trad_error})")

                if not ocr_success or not os.path.exists(output_path):
                    raise Exception("Nenhum motor de OCR conseguiu processar o arquivo.")

                # 🗂️ DIVISÃO POSTERIOR: Verificar se o arquivo precisa ser dividido
                file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                add_log(f"PDF processado: {file_size_mb:.2f} MB")

                if file_size_mb <= 5.0:
                    # Arquivo pequeno, não precisa dividir
                    progress_db[task_id]["final_file"] = output_filename
                    add_log("Arquivo dentro do limite. Divisão não necessária.")
                else:
                    # Arquivo grande, dividir em volumes de 5MB
                    add_log(f"Arquivo grande ({file_size_mb:.2f} MB). Iniciando divisão em volumes...")
                    progress_db[task_id]["status"] = "Preparando divisão em volumes..."
                    progress_db[task_id]["current"] = 0
                    progress_db[task_id]["total"] = 0

                    last_logged_page = 0

                    def on_page(current_page, total_pages):
                        progress_db[task_id]["current"] = current_page
                        progress_db[task_id]["total"] = total_pages
                        progress_db[task_id]["status"] = f"Dividindo volumes: {current_page}/{total_pages} páginas"

                        nonlocal last_logged_page
                        if current_page - last_logged_page >= 10 or current_page == total_pages:
                            add_log(f"Dividindo volumes: {current_page}/{total_pages} páginas")
                            last_logged_page = current_page

                    def on_volume(vol, added, size_mb):
                        add_log(f"Volume {vol} criado com {added} páginas ({size_mb:.2f} MB)")

                    # Divisão sem OCR (já processado)
                    import pathlib
                    result = split_pdf_only(
                        pathlib.Path(output_path),
                        pathlib.Path(UPLOAD_FOLDER), 
                        5.0,  # 5MB por volume
                        on_page=on_page,
                        on_volume=on_volume,
                        check_cancelled=lambda: progress_db[task_id]["cancelled"],
                    )

                    if result.get("cancelled"):
                        add_log("Divisão cancelada pelo usuário.")
                        progress_db[task_id]["status"] = "Cancelado"
                        return

                    # Buscar volumes criados
                    base_name = os.path.splitext(os.path.basename(output_path))[0]
                    volume_pattern = os.path.join(UPLOAD_FOLDER, f"{base_name}_VOL_*.pdf")
                    dividir_files = sorted(glob.glob(volume_pattern))

                    if dividir_files:
                        # Remover arquivo original grande
                        try:
                            os.remove(output_path)
                            add_log("Arquivo original removido após divisão.")
                        except:
                            pass

                        if len(dividir_files) > 1:
                            # Múltiplos volumes → ZIP
                            zip_name = f"volumes_{task_id}.zip"
                            zip_path = os.path.join(UPLOAD_FOLDER, zip_name)

                            total_volumes = len(dividir_files)
                            progress_db[task_id]["status"] = "Compactando ZIP..."
                            progress_db[task_id]["current"] = 0
                            progress_db[task_id]["total"] = total_volumes
                            add_log(f"Compactando {total_volumes} volumes em ZIP...")

                            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                                for idx, f in enumerate(dividir_files, start=1):
                                    arcname = os.path.basename(f)
                                    zipf.write(f, arcname)
                                    progress_db[task_id]["current"] = idx
                                    progress_db[task_id]["status"] = f"Compactando ZIP: {idx}/{total_volumes}"
                                    add_log(f"ZIP {idx}/{total_volumes}: {arcname}")

                            add_log("Compactação concluída.")
                            progress_db[task_id]["status"] = "Compactação concluída"
                            progress_db[task_id]["final_file"] = zip_name
                        else:
                            # Volume único
                            progress_db[task_id]["final_file"] = os.path.basename(dividir_files[0])
                    else:
                        # Divisão falhou, manter arquivo original
                        add_log("Divisão falhou. Mantendo arquivo original.")
                        progress_db[task_id]["final_file"] = output_filename

            else:
                output_filename = f"opt_{task_id}_{file.filename}"
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)

                add_log("Iniciando compressão...")
                progress_db[task_id]["status"] = "Compressão HP em andamento..."

                def callback(processed_count, total_pages):
                    progress_db[task_id]["current"] = processed_count + 1
                    progress_db[task_id]["total"] = total_pages
                    progress_db[task_id]["status"] = f"Compressão HP: {processed_count + 1}/{total_pages} páginas"
                    if (processed_count + 1) % 5 == 0 or (processed_count + 1) == total_pages:
                        add_log(f"Compressão HP: {processed_count + 1}/{total_pages} páginas.")

                def check_cancelled():
                    return progress_db[task_id]["cancelled"]

                processar_pdf_custom(input_path, output_path, config_map, callback, check_cancelled)
                progress_db[task_id]["final_file"] = output_filename

            elapsed = int(time.time() - progress_db[task_id]["start_time"])
            add_log(f"Processo concluído com sucesso em {elapsed}s.")
            progress_db[task_id]["status"] = "Concluído"

            secs = round(time.time() - start_ts, 2)
            log_uso(
                acao="concluído",
                modulo="processar",
                ip=cliente_ip,
                usuario=usuario,
                descricao="Processamento concluído",
                observacao=f"id_tarefa={task_id};secs={secs}",
            )

        except Exception as e:
            elapsed = int(time.time() - progress_db[task_id]["start_time"])
            add_log(f"ERRO após {elapsed}s: {str(e)}")
            progress_db[task_id]["status"] = "Falha no processamento"

    threading.Thread(target=worker_process, daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route("/progress/<task_id>")
def progress(task_id):
    def event_stream():
        last_log_len = 0
        while True:
            task = progress_db.get(task_id)
            if not task:
                break

            logs = task.get("logs", "")
            new_logs = []
            if len(logs) > last_log_len:
                new_logs = [l for l in logs[last_log_len:].splitlines() if l.strip()]
                last_log_len = len(logs)

            data = {
                "percent": (task["current"] / task["total"]) * 100 if task["total"] > 0 else 0,
                "status": task["status"],
                "logs": new_logs,
                "final_file": task.get("final_file"),
                "assinatura": task.get("assinatura"),
                "current": task.get("current", 0),
                "total": task.get("total", 0),
                "elapsed": int(time.time() - task.get("start_time", time.time())),
            }
            yield f"data: {json.dumps(data)}\n\n"

            if task["status"] in ["Concluído", "Falha no processamento", "Cancelado"]:
                break
            time.sleep(0.5)

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")


@app.route("/cancelar/<task_id>", methods=["POST"])
def cancelar_tarefa(task_id):
    task = progress_db.get(task_id)
    if not task:
        return jsonify({"error": "Tarefa não encontrada"}), 404

    if task["status"] in ["Concluído", "Falha no processamento", "Cancelado"]:
        return jsonify({"error": "Tarefa já finalizada"}), 400

    task["cancelled"] = True
    task["status"] = "Cancelado"
    task["logs"] += f"[{sao_paulo_time_str()}] Cancelamento solicitado.\n"

    # log opcional
    try:
        log_uso(
            acao="cancelado",
            modulo="cancelar",
            ip=request.remote_addr or "",
            usuario=get_user(),
            descricao="Cancelamento solicitado via botão",
            observacao=f"endpoint=/cancelar/{task_id}",
        )
    except Exception:
        pass

    return jsonify({"success": True, "message": "Tarefa cancelada"})


@app.route("/download/<task_id>")
def download_file(task_id):
    task = progress_db.get(task_id)
    if not task or task.get("status") != "Concluído":
        return "Arquivo não disponível.", 404

    filename = task.get("final_file")
    file_path = os.path.join(UPLOAD_FOLDER, filename)

    if os.path.exists(file_path):
        try:
            log_uso(
                acao="download",
                modulo="download",
                ip=request.remote_addr or "",
                usuario=get_user(),
                descricao="Download do resultado",
                observacao=f"id_tarefa={task_id}",
            )
        except Exception:
            pass

        clean_name = filename.replace(f"_{task_id}", "").replace(f"{task_id}_", "")
        return send_file(file_path, as_attachment=True, download_name=clean_name)

    return "Erro: Arquivo físico não encontrado.", 404


def cleanup_old_uploads(max_age_hours=24):
    import time as _time
    max_age_seconds = max_age_hours * 3600
    now = _time.time()

    try:
        for filename in os.listdir(UPLOAD_FOLDER):
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(filepath):
                age = now - os.path.getmtime(filepath)
                if age > max_age_seconds:
                    os.remove(filepath)
                    logging.info(f"Removido arquivo antigo: {filename}")
    except Exception as e:
        logging.warning(f"Erro ao limpar uploads antigos: {e}")


@app.route("/docs")
def como_usar():
    return render_template("como_usar.html")


@app.route("/feedback", methods=["POST"])
def feedback():
    data = request.get_json(force=True, silent=True) or {}
    stars = int(data.get("stars", 0) or 0)
    message = (data.get("message") or "").strip()
    module = (data.get("module") or "").strip()

    if stars < 1 or stars > 5:
        return jsonify({"ok": False, "error": "Estrelas inválidas (1..5)"}), 400
    if len(message) < 3:
        return jsonify({"ok": False, "error": "Mensagem muito curta"}), 400

    log_feedback(
        estrelas=stars,
        descricao=message,
        ip=request.remote_addr or "",
        usuario=get_user(),
        modulo=module,
        observacao="",
    )
    return jsonify({"ok": True})


@app.route("/admin")
def admin():
    metrics = compute_metrics()
    feedback_rows = read_tail_feedback(limit=200)
    uso_rows = read_tail_uso(limit=200)

    active = sum(
        1
        for _, t in progress_db.items()
        if t.get("status") not in ["Concluído", "Falha no processamento", "Cancelado"]
    )

    return render_template(
        "admin.html",
        metrics=metrics,
        feedback=feedback_rows[::-1],
        usage=uso_rows[::-1],
        active_tasks=active,
    )

if __name__ == "__main__":
    cleanup_old_uploads()
    cleanup_temp_dir()
    logging.info(f"Diretório temporário (RAM Disk): {temp_dir()}")
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
