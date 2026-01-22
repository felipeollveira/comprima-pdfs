# Usa uma imagem Python leve baseada em Debian
FROM python:3.10-slim-bullseye

# Instala dependências do sistema (Ghostscript, Tesseract, OCRmyPDF e utilitários)
RUN apt-get update && apt-get install -y \
    ghostscript \
    ocrmypdf \
    tesseract-ocr-por \
    tesseract-ocr-eng \
    libz-dev \
    libjpeg-dev \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho
WORKDIR /app

# Copia os requisitos e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY . .

# Cria a pasta de uploads
RUN mkdir -p uploads

# Expõe a porta do Flask
EXPOSE 5000

# Comando para rodar a aplicação ouvindo todas as interfaces
CMD ["python", "app.py"]