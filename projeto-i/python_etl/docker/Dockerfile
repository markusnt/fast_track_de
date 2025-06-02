# Imagem base
FROM python:3.11-slim

# Diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema (como libpq para o psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar arquivos do projeto
COPY ./etl.py /app/etl.py
COPY ./input /app/input

# Instalar dependências Python
RUN pip install --no-cache-dir pandas psycopg2-binary pymongo

# Comando padrão ao iniciar o container
CMD ["python", "etl.py"]
