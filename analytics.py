import os
from datetime import datetime
from pathlib import Path
from threading import Lock
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter

LOCK = Lock()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
USO_XLSX = DATA_DIR / "uso.xlsx"
FEEDBACK_XLSX = DATA_DIR / "feedback.xlsx"

HEADERS = ["Data", "Hora", "Usuário", "Ação", "Módulo", "Tempo", "Estrelas", "Descrição", "IP", "Observação"]

print(f"Diretório de uso criado em: {USO_XLSX.parent}")

def _ensure_parent_dir(file_path: str):
    parent = os.path.dirname(os.path.abspath(file_path))
    os.makedirs(parent, exist_ok=True)


def _agora_data_hora():
    now = datetime.now()
    return now.strftime("%d/%m/%Y"), now.strftime("%H:%M:%S")


def _setup_sheet(ws):
    ws.append(HEADERS)

    bold = Font(bold=True)
    center = Alignment(vertical="center")

    for cell in ws[1]:
        cell.font = bold
        cell.alignment = center

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(HEADERS))}1"
    ws.row_dimensions[1].height = 18

    widths = [12, 10, 18, 14, 16, 10, 45, 16, 30]
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w


def _append_xlsx(path: str, values: list):
    _ensure_parent_dir(path)

    with LOCK:
        if not os.path.exists(path):
            wb = Workbook()
            ws = wb.active
            ws.title = "Dados"
            _setup_sheet(ws)
            ws.append(values)
            wb.save(path)
            return

        wb = load_workbook(path)
        ws = wb.active

        if ws.max_row < 1 or (ws.max_row == 1 and ws["A1"].value != "Data"):
            ws.delete_rows(1, ws.max_row)
            _setup_sheet(ws)

        ws.append(values)
        wb.save(path)


# =========================
# USO (uso.xlsx)
# =========================
def log_uso(
    acao: str,
    modulo: str,
    ip: str,
    usuario: str = "",
    descricao: str = "",
    observacao: str = "",
):
    data, hora = _agora_data_hora()
    values = [
        data,
        hora,
        usuario or "",
        (acao or "").strip(),
        (modulo or "").strip(),
        "",  # Estrelas vazio no uso
        (descricao or "").strip(),
        ip or "",
        (observacao or "").strip(),
    ]
    _append_xlsx(USO_XLSX, values)


# =========================
# FEEDBACK (feedback.xlsx)
# =========================
def log_feedback(
    estrelas: int,
    descricao: str,
    ip: str,
    usuario: str = "",
    modulo: str = "",
    tempo: int = "",
    observacao: str = "",
):
    data, hora = _agora_data_hora()

    try:
        estrelas = int(estrelas)
    except Exception:
        estrelas = 0

    values = [
        data,
        hora,
        usuario or "",
        "feedback",
        (modulo or "").strip(),
        estrelas,
        (descricao or "").strip(),
        ip or "",
        (observacao or "").strip(),
    ]
    _append_xlsx(FEEDBACK_XLSX, values)


def _read_all_xlsx(path: str):
    if not os.path.exists(path):
        return []
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        return []

    headers = [h for h in rows[0]]
    out = []
    for r in rows[1:]:
        if not r:
            continue
        item = {}
        for i, h in enumerate(headers):
            item[str(h)] = r[i] if i < len(r) else ""
        out.append(item)
    return out


def read_tail_uso(limit: int = 200):
    rows = _read_all_xlsx(USO_XLSX)
    return rows[-limit:]


def read_tail_feedback(limit: int = 200):
    rows = _read_all_xlsx(FEEDBACK_XLSX)
    return rows[-limit:]


def compute_metrics():
    uso = _read_all_xlsx(USO_XLSX)
    feedback = _read_all_xlsx(FEEDBACK_XLSX)

    def norm(x):
        return (x or "").strip().lower()

    uploads = sum(1 for r in uso if norm(r.get("Ação")) == "upload")
    downloads = sum(1 for r in uso if norm(r.get("Ação")) == "download")
    inicios = sum(1 for r in uso if norm(r.get("Ação")) in ["início", "inicio"])
    concluidos = sum(1 for r in uso if norm(r.get("Ação")) in ["concluído", "concluido"])
    erros = sum(1 for r in uso if norm(r.get("Ação")) == "erro")
    cancelados = sum(1 for r in uso if norm(r.get("Ação")) == "cancelado")

    estrelas_list = []
    for r in feedback:
        try:
            estrelas_list.append(int(r.get("Estrelas") or 0))
        except Exception:
            pass

    media = round(sum(estrelas_list) / len(estrelas_list), 2) if estrelas_list else 0.0
    aprovacoes = sum(1 for s in estrelas_list if s >= 4)
    taxa_aprov = round((aprovacoes / len(estrelas_list)) * 100, 1) if estrelas_list else 0.0

    return {
        "uploads": uploads,
        "downloads": downloads,
        "inicios": inicios,
        "concluidos": concluidos,
        "erros": erros,
        "cancelados": cancelados,
        "feedback_count": len(feedback),
        "media_estrelas": media,
        "taxa_aprovacao_pct": taxa_aprov,
        "aprovacoes_4_ou_5": aprovacoes,
    }
