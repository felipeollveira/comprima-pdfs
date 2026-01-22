# Imagem base otimizada
FROM python:3.10-slim-bullseye

# Instalação de dependências de sistema para PDF e OCR
RUN apt-get update && apt-get install -y \
    ghostscript \
    ocrmypdf \
    tesseract-ocr-por \
    tesseract-ocr-eng \
    libz-dev \
    libjpeg-dev \
    pngquant \
    unpaper \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código fonte
COPY . .

# Garante permissões na pasta de uploads
RUN mkdir -p uploads && chmod 777 uploads

# Porta do Flask
EXPOSE 5000

# Executa o app
CMD ["python", "app.py"]