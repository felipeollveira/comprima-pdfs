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
from engines.force_ocr import split_volumes
from engines.execute_gs import processar_pdf_custom
from engines.signature import has_signature
from engines.ramdisk import temp_dir, cleanup_temp_dir
from engines.high_performance_ocr import process_pdf_high_performance
from engines.split_only import split_pdf_only
from engines.constants import MAX_DOC_MB, MAX_DOC_KB

# Alias para compatibilidade com código existente
MAX_MB = MAX_DOC_MB  # 5000 KB = 4.88 MB


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

    files = request.files.getlist("pdf")
    if not files or all(f.filename == "" for f in files):
        return jsonify({"error": "Nenhum arquivo enviado"}), 400

    MAX_BATCH = 3
    files = [f for f in files if f.filename][:MAX_BATCH]

    # Validação de tamanho total (limite: 499MB)
    MAX_FILE_SIZE_MB = 499
    total_size_bytes = request.content_length or 0

    # Salvar todos os arquivos e calcular tamanho
    config_map = json.loads(request.form.get("config_map", "{}"))
    extra_compress_pages = json.loads(request.form.get("extra_compress_pages", "[]"))
    # Validar: deve ser lista de inteiros
    extra_compress_pages = [int(p) for p in extra_compress_pages if isinstance(p, (int, float))]
    task_id = str(uuid.uuid4())
    saved_files = []  # [(input_path, original_name, size_mb), ...]

    for file in files:
        raw = file.read()
        file_size_mb = len(raw) / (1024 * 1024)
        file.seek(0)

        input_filename = f"{task_id}_{file.filename}"
        input_path = os.path.join(UPLOAD_FOLDER, input_filename)

        with open(input_path, "wb") as fp:
            fp.write(raw)

        saved_files.append((input_path, file.filename, file_size_mb))

    total_size_mb = sum(s for _, _, s in saved_files)

    log_uso(
        acao="upload",
        modulo="processar",
        ip=cliente_ip,
        usuario=usuario,
        descricao=f"{len(files)} arquivo(s) enviado(s) para processamento",
        observacao=f"endpoint={endpoint};files={len(saved_files)};in_mb={total_size_mb:.2f}",
    )

    if total_size_mb > MAX_FILE_SIZE_MB:
        # Limpar arquivos salvos
        for p, _, _ in saved_files:
            try: os.remove(p)
            except: pass
        log_uso(
            acao="erro",
            modulo="processar",
            ip=cliente_ip,
            usuario=usuario,
            descricao="Lote muito grande",
            observacao=f"endpoint={endpoint};in_mb={total_size_mb:.1f}",
        )
        return jsonify({"error": f"Lote muito grande ({total_size_mb:.1f}MB). Máximo: {MAX_FILE_SIZE_MB}MB"}), 413

    assinatura = False
    for input_path, _, _ in saved_files:
        try:
            if has_signature(input_path):
                assinatura = True
                break
        except Exception as e:
            logging.warning(f"Falha ao verificar assinatura digital: {e}")

    files_desc = ", ".join(f"{name} ({sz:.2f}MB)" for _, name, sz in saved_files)
    progress_db[task_id] = {
        "current": 0,
        "total": len(saved_files),
        "status": "Iniciando...",
        "stage": "prepare",
        "stage_label": "Iniciando",
        "stage_detail": "Preparando tarefas",
        "stage_percent": 0.0,
        "logs": f"[{sao_paulo_time_str()}] {len(saved_files)} arquivo(s) recebido(s): {files_desc}.\n",
        "final_file": None,
        "assinatura": assinatura,
        "cancelled": False,
        "start_time": time.time(),
        "file_index": 0,
        "file_count": len(saved_files),
        "file_names": [name for _, name, _ in saved_files],
        "current_file": "",
        "file_statuses": ["pending"] * len(saved_files),
    }

    def worker_process():
        import shutil as _shutil

        STAGE_RANGES = {
            "prepare": (1, 6),
            "ocr_pages": (6, 70),
            "page_compress": (70, 78),
            "merge": (78, 88),
            "extra_compress": (88, 92),
            "finalize": (92, 94),
            "split": (94, 97),
            "zip": (99, 99.7),
            "done": (100, 100),
        }

        def update_stage(stage, label, current=0, total=0, detail="", batch_label=""):
            task = progress_db.get(task_id)
            if not task:
                return

            c = max(0, int(current or 0))
            t = max(0, int(total or 0))
            start, end = STAGE_RANGES.get(stage, (0, 99))

            if stage == "done":
                stage_percent = 100.0
            else:
                ratio = min(1.0, (c / t)) if t > 0 else 0.0
                stage_percent = start + ((end - start) * ratio)
                stage_percent = min(stage_percent, 99.9)

            prefix = f"{batch_label} " if batch_label else ""
            task["stage"] = stage
            task["stage_label"] = label
            task["stage_detail"] = detail or ""
            task["stage_percent"] = round(stage_percent, 2)
            task["current"] = c
            task["total"] = t
            task["status"] = f"{prefix}{label}".strip()

        def add_log(msg):
            progress_db[task_id]["logs"] += f"[{sao_paulo_time_str()}] {msg}\n"

        start_ts = time.time()

        log_uso(
            acao="início",
            modulo="processar",
            ip=cliente_ip,
            usuario=usuario,
            descricao="Processamento iniciado",
            observacao=(
                f"id_tarefa={task_id};arquivos={len(saved_files)};"
                f"in_mb={total_size_mb:.2f}"
            ),
        )

        try:
            # OCR agora e obrigatorio para todos os processamentos.
            is_ocr = True
            is_dividir = any(int(v) == 6 for v in (config_map or {}).values())
            hp_level = _resolve_hp_level_from_config(config_map)
            result_files = []  # [(output_path, original_name), ...]

            for file_idx, (input_path, original_name, file_size_mb) in enumerate(saved_files, start=1):
                if progress_db[task_id]["cancelled"]:
                    add_log("Processamento cancelado pelo usuário.")
                    progress_db[task_id]["status"] = "Cancelado"
                    progress_db[task_id]["stage"] = "cancelled"
                    progress_db[task_id]["stage_label"] = "Cancelado"
                    progress_db[task_id]["stage_detail"] = "Cancelado pelo usuário"
                    return

                batch_label = f"[{file_idx}/{len(saved_files)}]" if len(saved_files) > 1 else ""
                progress_db[task_id]["file_index"] = file_idx
                progress_db[task_id]["current_file"] = original_name
                progress_db[task_id]["file_statuses"][file_idx - 1] = "processing"
                add_log(f"{batch_label} Processando: {original_name} ({file_size_mb:.2f}MB)")
                update_stage(
                    "prepare",
                    "Preparando OCR",
                    0,
                    1,
                    detail=f"Arquivo atual: {original_name}",
                    batch_label=batch_label,
                )

                output_filename = f"opt_{task_id}_{original_name}"
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)

                if is_ocr:
                    update_stage(
                        "prepare",
                        "OCR Alta Performance",
                        0,
                        1,
                        detail="Inicializando motor OCR",
                        batch_label=batch_label,
                    )

                    add_log(f"{batch_label} HP-OCR nível de compressão: {hp_level}")

                    last_hp_log = {"stage": None, "tick": 0}

                    def hp_callback(progress_payload, total_pages=None, _lbl=batch_label):
                        if isinstance(progress_payload, dict):
                            stage = progress_payload.get("stage") or "ocr_pages"
                            label = progress_payload.get("label") or "OCR"
                            current_page = int(progress_payload.get("current", 0) or 0)
                            total_pages_local = int(progress_payload.get("total", 0) or 0)
                            detail = progress_payload.get("detail", "")

                            if stage == "done":
                                stage = "finalize"
                                label = "Pós-processamento OCR"
                                current_page = 1
                                total_pages_local = 1

                            update_stage(
                                stage,
                                label,
                                current_page,
                                total_pages_local,
                                detail=detail,
                                batch_label=_lbl,
                            )

                            last_hp_log["tick"] += 1
                            if stage != last_hp_log["stage"]:
                                add_log(f"{_lbl} {label}: {detail or 'etapa iniciada'}")
                                last_hp_log["stage"] = stage
                            elif last_hp_log["tick"] % 20 == 0 and detail:
                                add_log(f"{_lbl} {label}: {detail}")
                            return

                        current_page = int(progress_payload or 0)
                        total_pages_local = int(total_pages or 0)
                        update_stage(
                            "ocr_pages",
                            "OCR por página",
                            current_page,
                            total_pages_local,
                            detail=f"{current_page}/{total_pages_local} páginas processadas",
                            batch_label=_lbl,
                        )
                        if total_pages_local and (current_page % 5 == 0 or current_page == total_pages_local):
                            add_log(f"{_lbl} OCR: {current_page}/{total_pages_local} páginas processadas.")

                    ocr_success = False
                    try:
                        result_path = process_pdf_high_performance(
                            input_path,
                            callback=hp_callback,
                            compression_level=hp_level,
                            extra_compress_pages=extra_compress_pages,
                            skip_extra_compression=is_dividir,
                        )
                        if os.path.abspath(result_path) != os.path.abspath(output_path):
                            _shutil.move(result_path, output_path)
                        add_log(f"{batch_label} HP-OCR concluído com sucesso.")
                        ocr_success = True

                    except Exception as hp_error:
                        add_log(f"{batch_label} HP-OCR falhou: {hp_error}. Tentando OCR tradicional...")
                        logging.warning(f"Fallback OCR tradicional: {hp_error}")
                        try:
                            update_stage(
                                "prepare",
                                "OCR tradicional",
                                0,
                                1,
                                detail="HP-OCR falhou, aplicando fallback",
                                batch_label=batch_label,
                            )

                            def callback(processed_count, total_pages, _lbl=batch_label):
                                update_stage(
                                    "ocr_pages",
                                    "OCR tradicional",
                                    processed_count + 1,
                                    total_pages,
                                    detail=f"{processed_count + 1}/{total_pages} páginas",
                                    batch_label=_lbl,
                                )
                                add_log(f"{_lbl} OCR tradicional: {processed_count + 1}/{total_pages} páginas.")

                            def check_cancelled():
                                return progress_db[task_id]["cancelled"]

                            processar_pdf_custom(
                                input_path,
                                output_path,
                                config_map,
                                callback,
                                check_cancelled,
                                extra_compress_pages=extra_compress_pages,
                            )
                            add_log(f"{batch_label} OCR tradicional concluído.")
                            ocr_success = True
                        except Exception as trad_error:
                            add_log(f"{batch_label} OCR tradicional também falhou: {trad_error}")
                            raise Exception(f"Falha em ambos motores OCR para {original_name}")

                    if not ocr_success or not os.path.exists(output_path):
                        raise Exception(f"Nenhum motor OCR conseguiu processar {original_name}.")

                    # Divisão posterior se necessário
                    out_size_mb = os.path.getsize(output_path) / (1024 * 1024)
                    add_log(f"{batch_label} PDF processado: {out_size_mb:.2f} MB")

                    if is_dividir or out_size_mb > MAX_MB:
                        if is_dividir:
                            add_log(f"{batch_label} Aplicando OCR + DIVIDIR (nível 6). Dividindo em volumes...")
                        else:
                            add_log(f"{batch_label} Arquivo grande ({out_size_mb:.2f} MB). Dividindo em volumes...")
                        update_stage(
                            "split",
                            "Dividindo volumes",
                            0,
                            1,
                            detail="Calculando cortes por volume",
                            batch_label=batch_label,
                        )

                        last_logged_page = 0

                        def on_page(current_page, total_pages, _lbl=batch_label):
                            update_stage(
                                "split",
                                "Dividindo volumes",
                                current_page,
                                total_pages,
                                detail=f"{current_page}/{total_pages} páginas analisadas",
                                batch_label=_lbl,
                            )
                            nonlocal last_logged_page
                            if current_page - last_logged_page >= 10 or current_page == total_pages:
                                add_log(f"{_lbl} Dividindo: {current_page}/{total_pages} páginas")
                                last_logged_page = current_page

                        def on_volume(vol, added, size_mb, _lbl=batch_label):
                            add_log(f"{_lbl} Volume {vol}: {added} páginas ({size_mb:.2f} MB)")

                        result = split_pdf_only(
                            pathlib.Path(output_path),
                            pathlib.Path(UPLOAD_FOLDER),
                            MAX_MB,
                            on_page=on_page,
                            on_volume=on_volume,
                            check_cancelled=lambda: progress_db[task_id]["cancelled"],
                        )

                        if result.get("cancelled"):
                            add_log("Divisão cancelada pelo usuário.")
                            progress_db[task_id]["status"] = "Cancelado"
                            progress_db[task_id]["stage"] = "cancelled"
                            progress_db[task_id]["stage_label"] = "Cancelado"
                            progress_db[task_id]["stage_detail"] = "Cancelado durante divisão"
                            return

                        base_name = os.path.splitext(os.path.basename(output_path))[0]
                        volume_pattern = os.path.join(UPLOAD_FOLDER, f"{base_name}_VOL_*.pdf")
                        dividir_files = sorted(glob.glob(volume_pattern))

                        if dividir_files:
                            try: os.remove(output_path)
                            except: pass

                            # Não recomprime volumes aqui: eles já passaram por OCR + compressão antes da divisão.
                            for vol_idx, vf in enumerate(dividir_files, 1):
                                if progress_db[task_id]["cancelled"]:
                                    progress_db[task_id]["status"] = "Cancelado"
                                    progress_db[task_id]["stage"] = "cancelled"
                                    progress_db[task_id]["stage_label"] = "Cancelado"
                                    progress_db[task_id]["stage_detail"] = "Cancelado após divisão"
                                    return

                                result_files.append((vf, os.path.basename(vf)))

                            update_stage(
                                "split",
                                "Divisão concluída",
                                len(dividir_files),
                                len(dividir_files),
                                detail=f"{len(dividir_files)} volume(s) gerado(s)",
                                batch_label=batch_label,
                            )
                            add_log(f"{batch_label} {len(dividir_files)} volume(s) gerado(s). Compressão de volumes ignorada.")
                        else:
                            result_files.append((output_path, output_filename))
                    else:
                        result_files.append((output_path, output_filename))

                else:
                    # Compressão sem OCR
                    add_log(f"{batch_label} Iniciando compressão...")
                    update_stage(
                        "prepare",
                        "Compressão HP",
                        0,
                        1,
                        detail="Inicializando compressão sem OCR",
                        batch_label=batch_label,
                    )

                    def callback(processed_count, total_pages, _lbl=batch_label):
                        update_stage(
                            "ocr_pages",
                            "Compressão por página",
                            processed_count + 1,
                            total_pages,
                            detail=f"{processed_count + 1}/{total_pages} páginas",
                            batch_label=_lbl,
                        )
                        if (processed_count + 1) % 5 == 0 or (processed_count + 1) == total_pages:
                            add_log(f"{_lbl} Compressão: {processed_count + 1}/{total_pages} páginas.")

                    def check_cancelled():
                        return progress_db[task_id]["cancelled"]

                    processar_pdf_custom(
                        input_path,
                        output_path,
                        config_map,
                        callback,
                        check_cancelled,
                        extra_compress_pages=extra_compress_pages,
                    )
                    result_files.append((output_path, output_filename))

                progress_db[task_id]["file_statuses"][file_idx - 1] = "done"
                add_log(f"{batch_label} OK ✔")

            def _safe_size_mb(path):
                try:
                    return os.path.getsize(path) / (1024 * 1024)
                except Exception:
                    return 0.0

            # --- Resultado final ---
            if len(result_files) == 1:
                final_path = result_files[0][0]
                progress_db[task_id]["final_file"] = os.path.basename(final_path)
            else:
                zip_name = f"lote_{task_id}.zip"
                zip_path = os.path.join(UPLOAD_FOLDER, zip_name)
                total_items = len(result_files)
                update_stage("zip", "Compactando ZIP", 0, total_items, detail=f"0/{total_items} itens")
                add_log(f"Compactando {total_items} arquivo(s) em ZIP...")

                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                    for idx, (fpath, fname) in enumerate(result_files, start=1):
                        arcname = os.path.basename(fpath)
                        # Remove task_id prefix from archive name for cleaner output
                        clean = arcname.replace(f"opt_{task_id}_", "").replace(f"{task_id}_", "")
                        zipf.write(fpath, clean)
                        update_stage("zip", "Compactando ZIP", idx, total_items, detail=f"{idx}/{total_items} itens")
                        add_log(f"ZIP {idx}/{total_items}: {clean}")

                add_log("Compactação concluída.")
                progress_db[task_id]["final_file"] = zip_name
                final_path = zip_path

            elapsed = int(time.time() - progress_db[task_id]["start_time"])
            add_log(f"Processo concluído com sucesso em {elapsed}s.")
            update_stage("done", "Concluído", 1, 1, detail=f"Tempo total: {elapsed}s")
            progress_db[task_id]["status"] = "Concluído"

            secs = round(time.time() - start_ts, 2)
            out_mb = round(_safe_size_mb(final_path), 2)
            ratio_pct = round((out_mb / total_size_mb) * 100, 1) if total_size_mb else 0.0
            log_uso(
                acao="concluído",
                modulo="processar",
                ip=cliente_ip,
                usuario=usuario,
                descricao="Processamento concluído",
                observacao=(
                    f"id_tarefa={task_id};secs={secs};in_mb={total_size_mb:.2f};"
                    f"out_mb={out_mb:.2f};ratio_pct={ratio_pct}"
                ),
                tempo=secs,
            )

        except Exception as e:
            elapsed = int(time.time() - progress_db[task_id]["start_time"])
            add_log(f"ERRO após {elapsed}s: {str(e)}")
            progress_db[task_id]["status"] = "Falha no processamento"
            progress_db[task_id]["stage"] = "error"
            progress_db[task_id]["stage_label"] = "Falha"
            progress_db[task_id]["stage_detail"] = str(e)
            progress_db[task_id]["stage_percent"] = min(99.0, float(progress_db[task_id].get("stage_percent", 0) or 0))

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
                "percent": task.get("stage_percent") if task.get("stage_percent") is not None else ((task["current"] / task["total"]) * 100 if task["total"] > 0 else 0),
                "status": task["status"],
                "stage": task.get("stage", ""),
                "stage_label": task.get("stage_label", ""),
                "stage_detail": task.get("stage_detail", ""),
                "stage_percent": task.get("stage_percent", 0),
                "logs": new_logs,
                "final_file": task.get("final_file"),
                "assinatura": task.get("assinatura"),
                "current": task.get("current", 0),
                "total": task.get("total", 0),
                "elapsed": int(time.time() - task.get("start_time", time.time())),
                "file_index": task.get("file_index", 0),
                "file_count": task.get("file_count", 1),
                "file_names": task.get("file_names", []),
                "current_file": task.get("current_file", ""),
                "file_statuses": task.get("file_statuses", []),
            }
            yield f"data: {json.dumps(data)}\n\n"

            if task["status"] in ["Concluído", "Falha no processamento", "Cancelado"]:
                break
            time.sleep(0.5)

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")


@app.route("/progress_json/<task_id>")
def progress_json(task_id):
    task = progress_db.get(task_id)
    if not task:
        return jsonify({"error": "Tarefa não encontrada"}), 404

    data = {
        "percent": task.get("stage_percent") if task.get("stage_percent") is not None else ((task["current"] / task["total"]) * 100 if task["total"] > 0 else 0),
        "status": task["status"],
        "stage": task.get("stage", ""),
        "stage_label": task.get("stage_label", ""),
        "stage_detail": task.get("stage_detail", ""),
        "stage_percent": task.get("stage_percent", 0),
        "logs": [],
        "final_file": task.get("final_file"),
        "assinatura": task.get("assinatura"),
        "current": task.get("current", 0),
        "total": task.get("total", 0),
        "elapsed": int(time.time() - task.get("start_time", time.time())),
        "file_index": task.get("file_index", 0),
        "file_count": task.get("file_count", 1),
        "file_names": task.get("file_names", []),
        "current_file": task.get("current_file", ""),
        "file_statuses": task.get("file_statuses", []),
    }
    return jsonify(data)


@app.route("/cancelar/<task_id>", methods=["POST"])
def cancelar_tarefa(task_id):
    task = progress_db.get(task_id)
    if not task:
        return jsonify({"error": "Tarefa não encontrada"}), 404

    if task["status"] in ["Concluído", "Falha no processamento", "Cancelado"]:
        return jsonify({"error": "Tarefa já finalizada"}), 400

    task["cancelled"] = True
    task["status"] = "Cancelado"
    task["stage"] = "cancelled"
    task["stage_label"] = "Cancelado"
    task["stage_detail"] = "Cancelamento solicitado"
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
            try:
                out_mb = os.path.getsize(file_path) / (1024 * 1024)
            except Exception:
                out_mb = None
            log_uso(
                acao="download",
                modulo="download",
                ip=request.remote_addr or "",
                usuario=get_user(),
                descricao="Download do resultado",
                observacao=(
                    f"id_tarefa={task_id};out_mb={out_mb:.2f}"
                    if out_mb is not None
                    else f"id_tarefa={task_id}"
                ),
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

@app.route("/test")
def teste_pdf():
    return render_template("teste_pdf.html")


def _analisar_paginas_pdf_backend(pdf_path: str):
    """
    Analisa páginas usando o mesmo critério base do backend:
    tamanho do PDF de página isolada (em KB) e presença de texto pesquisável.
    """
    import io
    import fitz
    import pikepdf

    LIMIT_KB = 500
    MIN_TEXT_CHARS = 50
    page_results = []

    with pikepdf.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for idx in range(total_pages):
            single = pikepdf.Pdf.new()
            single.pages.append(pdf.pages[idx])

            buf = io.BytesIO()
            single.save(buf)
            size_kb = buf.tell() / 1024

            page_results.append(
                {
                    "page": idx + 1,
                    "size_kb": round(size_kb, 2),
                    "exceeds": size_kb > LIMIT_KB,
                    "has_ocr": False,
                    "text_chars": 0,
                }
            )

    with fitz.open(pdf_path) as doc:
        for idx, page in enumerate(doc):
            text = page.get_text().strip()
            text_chars = len(text)
            has_ocr = text_chars >= MIN_TEXT_CHARS

            if idx < len(page_results):
                page_results[idx]["has_ocr"] = has_ocr
                page_results[idx]["text_chars"] = text_chars

    return page_results


@app.route("/test/analisar", methods=["POST"])
def teste_pdf_analisar():
    if "pdf" not in request.files:
        return jsonify({"ok": False, "error": "Nenhum arquivo enviado"}), 400

    file = request.files["pdf"]
    if not file or not file.filename:
        return jsonify({"ok": False, "error": "Arquivo inválido"}), 400

    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "Envie apenas arquivo PDF"}), 400

    tmp_path = os.path.join(temp_dir(), f"test_{uuid.uuid4().hex}.pdf")

    try:
        file.save(tmp_path)
        page_results = _analisar_paginas_pdf_backend(tmp_path)

        total = len(page_results)
        warnings = sum(1 for p in page_results if p["exceeds"])
        ocr_pages = sum(1 for p in page_results if p["has_ocr"])

        return jsonify(
            {
                "ok": True,
                "total": total,
                "limit_kb": 500,
                "measurement": "single_page_pdf_bytes",
                "measurement_label": "Tamanho da página isolada em PDF (método backend)",
                "page_results": page_results,
                "summary": {
                    "ok_count": total - warnings,
                    "warning_count": warnings,
                    "ocr_count": ocr_pages,
                },
            }
        )
    except Exception as e:
        logging.exception("Falha ao analisar PDF no endpoint /test/analisar")
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass



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
    
    MAX_SIZE = 100 * 1024 * 1024 

    from waitress import serve
    # logging.info("Servidor Xeon configurado com 16 threads de rede.")
    
    serve(
        app, 
        #host="0.0.0.0", 
        port=5000, 
        threads=16, 
        max_request_body_size=MAX_SIZE,
        channel_timeout=400  
    )
