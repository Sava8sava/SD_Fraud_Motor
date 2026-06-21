FROM python:3.11-slim

WORKDIR /app

# Copia a lista de dependências e instala no container
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o código do projeto para dentro do container
COPY . .

# Por padrão, o container não faz nada, nós definiremos o comando no docker-compose
CMD ["python"]
