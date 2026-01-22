
import shutil

def localizar_gs() -> str:
    """Localiza o executável do Ghostscript no sistema."""
    gs_path = shutil.which('gswin64c') or shutil.which('gswin32c') or shutil.which('gs')
    if not gs_path:
        raise FileNotFoundError("Ghostscript não encontrado. Instale o Ghostscript e tente novamente.")
    return gs_path
