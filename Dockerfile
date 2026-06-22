FROM python:3.11-slim

WORKDIR /app

# Instala dependências do sistema operacional necessárias para compilar a confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia a lista de dependências e instala no container
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do projeto para dentro do container
COPY . .

CMD ["python"]
