FROM python:3.11-slim

WORKDIR /app

# Instalar git-lfs para descargar archivos grandes
RUN apt-get update && apt-get install -y git git-lfs curl && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cache buster - cambiar para forzar rebuild
ARG CACHEBUST=3

# Copiar código
COPY . .

# Descargar el archivo parquet desde GitHub LFS
# Si el archivo es un pointer LFS, lo descargamos directamente
RUN if [ -f data/censo_reducido.parquet ] && [ $(stat -c%s data/censo_reducido.parquet 2>/dev/null || stat -f%z data/censo_reducido.parquet) -lt 1000 ]; then \
    echo "Archivo LFS detectado, descargando..." && \
    cd /app && git init && git lfs install && \
    git remote add origin https://github.com/sebaduenas/anonimizacion-api.git && \
    git lfs pull --include="data/censo_reducido.parquet" || \
    echo "Descarga LFS falló, intentando curl..." && \
    curl -L -o data/censo_reducido.parquet "https://media.githubusercontent.com/media/sebaduenas/anonimizacion-api/main/data/censo_reducido.parquet"; \
    fi

# Verificar que el archivo existe y tiene tamaño razonable
RUN ls -la data/ && \
    if [ -f data/censo_reducido.parquet ]; then \
    echo "Archivo parquet encontrado: $(ls -lh data/censo_reducido.parquet)"; \
    else echo "ERROR: Archivo parquet no encontrado"; exit 1; \
    fi

# Exponer puerto (Railway usa variable PORT)
EXPOSE ${PORT:-8000}

# Comando de inicio - usa $PORT de Railway o 8000 por defecto
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
