"""
Módulo utilitário para uso de /dev/shm (RAM Disk) como diretório
temporário de processamento, acelerando I/O de arquivos intermediários.

Fallback automático para tempfile.gettempdir() em sistemas sem /dev/shm.
"""
import os
import logging
import tempfile

# Diretório de trabalho dentro do RAM Disk
_SHM_BASE = "/dev/shm"
_APP_SUBDIR = "pdf-optimizer"

def get_temp_dir():
    """
    Retorna o melhor diretório temporário disponível para processamento.
    Prioriza /dev/shm (RAM) para máxima velocidade de I/O.
    Fallback para o diretório temporário do sistema.
    """
    shm_path = os.path.join(_SHM_BASE, _APP_SUBDIR)

    if os.path.isdir(_SHM_BASE) and os.access(_SHM_BASE, os.W_OK):
        os.makedirs(shm_path, exist_ok=True)
        logging.info(f"Usando RAM Disk para temp: {shm_path}")
        return shm_path

    fallback = os.path.join(tempfile.gettempdir(), _APP_SUBDIR)
    os.makedirs(fallback, exist_ok=True)
    logging.info(f"RAM Disk indisponível. Usando fallback: {fallback}")
    return fallback


# Cache do diretório para evitar checagens repetidas
_cached_temp_dir = None

def temp_dir():
    """Retorna o diretório temporário cacheado (avaliado uma única vez)."""
    global _cached_temp_dir
    if _cached_temp_dir is None:
        _cached_temp_dir = get_temp_dir()
    return _cached_temp_dir


def cleanup_temp_dir():
    """Remove todos os arquivos temporários do diretório de processamento."""
    d = temp_dir()
    removed = 0
    try:
        for f in os.listdir(d):
            fp = os.path.join(d, f)
            if os.path.isfile(fp):
                os.remove(fp)
                removed += 1
        if removed:
            logging.info(f"Limpeza do RAM Disk: {removed} arquivo(s) removido(s)")
    except Exception as e:
        logging.warning(f"Erro ao limpar diretório temporário: {e}")
