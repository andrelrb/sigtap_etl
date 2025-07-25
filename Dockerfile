# Dockerfile
# Versão: 1.0
# Descrição: Define a imagem Docker para a aplicação Flask do SIGTAP ETL.

# Use uma imagem base oficial do Python
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de dependências primeiro para aproveitar o cache do Docker
COPY requirements.txt requirements.txt

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o resto do projeto para o diretório de trabalho no contêiner
COPY . .

# Expõe a porta que o Flask vai usar
EXPOSE 5000

# Comando para iniciar a aplicação quando o contêiner rodar
CMD ["python", "run_app.py"]