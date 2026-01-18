# API de Demostración de Anonimización

Backend FastAPI que demuestra la imposibilidad de anonimización usando datos censales de Chile.

## Requisitos

- Python 3.11+
- ~1GB RAM (para cargar el parquet en memoria)

## Instalación local

```bash
# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o: venv\Scripts\activate  # Windows

# Instalar dependencias
pip install -r requirements.txt

# Crear carpeta de datos y copiar el parquet
mkdir -p data
cp /ruta/a/censo_reducido.parquet data/

# Ejecutar
python main.py
```

La API estará disponible en http://localhost:8000

## Endpoints

| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/` | Health check |
| GET | `/docs` | Documentación Swagger |
| GET | `/opciones` | Valores para los selects del formulario |
| GET | `/comunas/{region_id}` | Comunas de una región |
| POST | `/buscar` | Buscar coincidencias de un perfil |
| POST | `/buscar-progresivo` | Embudo de anonimato paso a paso |

## Deploy en Railway

1. Fork/push este repo a GitHub
2. Ir a [railway.app](https://railway.app)
3. New Project → Deploy from GitHub
4. Seleccionar este repositorio
5. En la pestaña "Variables", agregar:
   - `DATA_PATH=data/censo_reducido.parquet`
6. Subir el archivo `censo_reducido.parquet` a la carpeta `data/` del proyecto en Railway

## Variables de entorno

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DATA_PATH` | `data/censo_reducido.parquet` | Ruta al archivo de datos |

## Licencia

Desarrollado por Inteligencia Digital.
