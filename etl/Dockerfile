FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Dependencias para ODBC y compilacion
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 apt-transport-https ca-certificates \
    unixodbc unixodbc-dev gcc g++ build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instalar Microsoft ODBC para SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

CMD ["python", "etl_run.py"]
