"""
Constantes centralizadas para o sistema de otimização de PDF.

Todos os limites e configurações globais devem ser definidos aqui
para garantir consistência entre os módulos.
"""

# ══════════════════════════════════════════════════════════════════════════════
#  LIMITES DE TAMANHO
# ══════════════════════════════════════════════════════════════════════════════

# Limite por página (em KB e bytes)
MAX_PAGE_KB = 500
MAX_PAGE_BYTES = MAX_PAGE_KB * 1024  # 512.000 bytes

# Limite por documento/volume (em KB, MB e bytes)
MAX_DOC_KB = 5000  # 5000 KB é o limite real
MAX_DOC_MB = MAX_DOC_KB / 1024  # 4.8828125 MB (não arredondar para 5!)
MAX_DOC_BYTES = MAX_DOC_KB * 1024  # 5.120.000 bytes

# Margem de segurança para absorver overhead do OCR e merge
SAFETY_MARGIN = 0.90  # 10% de folga
MAX_DOC_MB_SAFE = MAX_DOC_MB * SAFETY_MARGIN  # ~4.39 MB
MAX_DOC_BYTES_SAFE = int(MAX_DOC_BYTES * SAFETY_MARGIN)

# ══════════════════════════════════════════════════════════════════════════════
#  LIMITES POR MODO DE OPERAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

# Modo normal: compressão agressiva por página
PAGE_SIZE_LIMIT = MAX_PAGE_BYTES  # 500 KB

# Modo dividir: compressão mais leve (a divisão distribuirá o tamanho)
# Reduzido de 1.5 MB para 1 MB para garantir que volumes não excedam 5 MB
PAGE_SIZE_LIMIT_SPLIT = 1000 * 1024  # 1 MB 

# ══════════════════════════════════════════════════════════════════════════════
#  THRESHOLDS DE COMPRESSÃO EXTRA
# ══════════════════════════════════════════════════════════════════════════════

# Threshold para acionar compressão extra pós-merge
# Usa limite com margem de segurança (~4.39 MB)
EXTRA_COMPRESS_THRESHOLD_MB = MAX_DOC_MB_SAFE

# Ajustes da compressão extra para páginas selecionadas manualmente.
# Valores moderados para ficar "um pouco mais" agressivo sem degradar demais.
EXTRA_SELECTED_DPI_SCALE = 0.75
EXTRA_SELECTED_QUALITY_DELTA = 8

# ══════════════════════════════════════════════════════════════════════════════
#  PASSADAS DE COMPRESSÃO PROGRESSIVA
# ══════════════════════════════════════════════════════════════════════════════

# Modo normal: (dpi, jpeg_quality) - até atingir 500 KB
DPI_PASSES_NORMAL = [
    (150, 90),  # 1ª: boa qualidade, reduz ~73% em scans
    (120, 80),  # 2ª: mais agressivo
    (105, 70),  # 3ª: agressivo
    (70, 50),  # 4ª: último recurso (NOVO)
]

# Modo dividir: (dpi, jpeg_quality) - até atingir 1 MB
DPI_PASSES_SPLIT = [
    (180, 85),  # 1ª: alta qualidade
    (150, 70),  # 2ª: boa qualidade
    (140, 65),  # 3ª: média qualidade (NOVO)
]

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURAÇÕES DE PROCESSAMENTO
# ══════════════════════════════════════════════════════════════════════════════

# Workers para processamento paralelo
# REDUZIDO: PDFs com 400+ páginas precisam menos paralelismo para evitar OOM
MAX_WORKERS = 10  # Base, mas será limitado a 8 em execute_gs.py para PDFs grandes

# Limite de páginas para ativar modo "big file" (reduz workers automaticamente)
BIG_FILE_PAGE_THRESHOLD = 350  # Acima disto, limita a 8 workers

# GC agressivo para PDFs grandes
GC_AGGRESSIVE_LARGE_FILES = True  # Force gc.collect() a cada página (não a cada 10)

# Configurações do Tesseract
TESSERACT_LANG = "por+eng"
TESSERACT_DPI = 200

# Configurações do Ghostscript
GS_RENDERING_THREADS = 10
GS_BUFFER_SPACE = 1_000_000_000  # 1 GB

# ══════════════════════════════════════════════════════════════════════════════
#  VALIDAÇÃO DE PDF E IMAGENS
# ══════════════════════════════════════════════════════════════════════════════

# Tamanho mínimo para considerar PDF válido (1 KB)
MIN_PDF_SIZE_BYTES = 1024

# Espaço mínimo em RAM Disk para operação segura (500 MB)
MIN_RAMDISK_SPACE_MB = 500

# Timeout para validação de PNG (em segundos)
PNG_VALIDATION_TIMEOUT = 5

# Timeout para validação de PDF (em segundos)
PDF_VALIDATION_TIMEOUT = 5


# ══════════════════════════════════════════════════════════════════════════════
#  OCR
# ══════════════════════════════════════════════════════════════════════════════

# Limite de caracteres para decidir se página precisa de OCR
LIMITE_CHARS_OCR = 50

# Percentual mínimo de área de imagem para considerar página como scan
MIN_IMAGE_AREA_RATIO = 0.30  # 30% da área da página
