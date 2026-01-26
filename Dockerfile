# ESTÁGIO 1: Compilação do jbig2enc
FROM python:3.10-slim-bullseye AS builder

RUN apt-get update && apt-get install -y \
    git build-essential autoconf automake libtool libleptonica-dev zlib1g-dev

RUN git clone https://github.com/agl/jbig2enc /tmp/jbig2enc \
    && cd /tmp/jbig2enc \
    && libtoolize --force && aclocal && autoheader && automake --add-missing && autoconf \
    && ./configure && make && make install

# ESTÁGIO 2: Imagem Final do App
FROM python:3.10-slim-bullseye

# Instalação de dependências de sistema
RUN apt-get update && apt-get install -y \
    ghostscript \
    ocrmypdf \
    tesseract-ocr-por \
    tesseract-ocr-eng \
    libz-dev \
    libjpeg-dev \
    libpng-dev \
    pngquant \
    unpaper \
    libleptonica-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia o jbig2enc do estágio de build
COPY --from=builder /usr/local/bin/jbig2 /usr/local/bin/
COPY --from=builder /usr/local/lib/libjbig2enc.* /usr/local/lib/
RUN ldconfig

WORKDIR /app

# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código fonte
COPY . .

# Configurações de ambiente e permissões
RUN mkdir -p uploads && chmod 777 uploads
ENV PYTHONUNBUFFERED=1

EXPOSE 5000

CMD ["python", "app.py"]