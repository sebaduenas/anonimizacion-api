"""
API de Demostración de (Im)posibilidad de Anonimización
Backend FastAPI para consultar el Censo de Chile

Desarrollado por Inteligencia Digital
https://inteligencia-digital.vercel.app
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import os
import sys
from comunas import COMUNAS, get_comuna_name

# Logging inmediato para debugging
print(f"🚀 Iniciando servidor...", flush=True)
print(f"📍 Python: {sys.version}", flush=True)
print(f"📍 Puerto: {os.getenv('PORT', '8000')}", flush=True)

# ============================================
# CONFIGURACIÓN
# ============================================

app = FastAPI(
    title="API de Anonimización",
    description="Demuestra la imposibilidad de anonimización con datos censales de Chile",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

print("✓ FastAPI app creada", flush=True)

# ============================================
# HEALTH CHECK (responde INMEDIATAMENTE, sin esperar datos)
# ============================================

@app.get("/health")
async def health_check():
    """Endpoint de health check para Railway - responde sin esperar datos"""
    global df
    return {
        "status": "healthy", 
        "service": "anonimizacion-api",
        "data_loaded": df is not None and len(df) > 0 if df is not None else False
    }

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {"message": "API de Anonimización funcionando", "docs": "/docs"}

# CORS - permitir requests desde tu frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:4321",
        "https://inteligencia-digital.vercel.app",
        "https://*.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# CARGAR DATOS AL INICIAR (OPTIMIZADO PARA MEMORIA)
# ============================================

DATA_PATH = os.getenv("DATA_PATH", "data/censo_reducido.parquet")
df: pd.DataFrame = None

# Solo las columnas que necesitamos
COLUMNAS_NECESARIAS = [
    'region', 'comuna', 'sexo', 'edad_quinquenal',
    'p23_est_civil', 'cine11', 'sit_fuerza_trabajo',
    'cod_ciuo', 'p44_lug_trab', 'p45_medio_transporte'
]


@app.on_event("startup")
async def load_data():
    """Carga el parquet en memoria al iniciar la API (optimizado)"""
    global df
    print(f"📂 Intentando cargar datos desde: {DATA_PATH}", flush=True)
    
    # Verificar que el archivo existe
    if not os.path.exists(DATA_PATH):
        print(f"✗ ARCHIVO NO ENCONTRADO: {DATA_PATH}", flush=True)
        print(f"📍 Directorio actual: {os.getcwd()}", flush=True)
        print(f"📍 Contenido de data/: {os.listdir('data') if os.path.exists('data') else 'NO EXISTE'}", flush=True)
        df = pd.DataFrame()
        return
    
    file_size = os.path.getsize(DATA_PATH) / (1024 * 1024)
    print(f"📂 Archivo encontrado: {file_size:.1f} MB", flush=True)
    
    try:
        # Cargar solo columnas necesarias
        print(f"📊 Cargando columnas: {COLUMNAS_NECESARIAS}", flush=True)
        df = pd.read_parquet(DATA_PATH, columns=COLUMNAS_NECESARIAS)
        print(f"✓ Parquet leído: {len(df):,} registros", flush=True)
        
        # Convertir a tipos más eficientes en memoria
        for col in df.columns:
            # Convertir a int16 (suficiente para nuestros valores)
            if df[col].dtype in ['int64', 'Int64']:
                df[col] = df[col].fillna(-1).astype('int16')
            elif df[col].dtype == 'float64':
                df[col] = df[col].fillna(-1).astype('int16')
        
        print(f"✓ Datos optimizados: {len(df):,} registros", flush=True)
        print(f"✓ Columnas: {list(df.columns)}", flush=True)
        print(f"✓ Memoria usada: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB", flush=True)
        print(f"🎉 API lista para recibir requests!", flush=True)
    except Exception as e:
        print(f"✗ Error cargando datos: {e}", flush=True)
        import traceback
        traceback.print_exc()
        df = pd.DataFrame()


# ============================================
# MODELOS PYDANTIC
# ============================================

class PerfilBusqueda(BaseModel):
    """Perfil demográfico para buscar coincidencias"""
    region: Optional[int] = None
    comuna: Optional[int] = None
    sexo: Optional[int] = None
    edad_quinquenal: Optional[int] = None
    p23_est_civil: Optional[int] = None
    cine11: Optional[int] = None
    sit_fuerza_trabajo: Optional[int] = None
    cod_ciuo: Optional[int] = None
    p44_lug_trab: Optional[int] = None
    p45_medio_transporte: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "region": 13,
                "comuna": 13101,
                "sexo": 1,
                "edad_quinquenal": 30,
                "p23_est_civil": 8,
                "cine11": 9,
                "sit_fuerza_trabajo": 1
            }
        }


class ResultadoBusqueda(BaseModel):
    """Resultado de la búsqueda de perfil"""
    n_coincidencias: int
    total_poblacion: int
    porcentaje: float
    k_anonimidad: int
    es_unico: bool
    mensaje: str
    variables_usadas: list[str]


class ResultadoProgresivo(BaseModel):
    """Un paso del embudo de anonimato"""
    paso: int
    variable: str
    variable_label: str
    valor: Optional[int]
    valor_label: str
    n_coincidencias: int
    porcentaje: float


# ============================================
# DICCIONARIOS DE ETIQUETAS
# ============================================

VARIABLE_LABELS = {
    "region": "Región",
    "comuna": "Comuna",
    "sexo": "Sexo",
    "edad_quinquenal": "Tramo de edad",
    "p23_est_civil": "Estado civil",
    "cine11": "Nivel educacional",
    "sit_fuerza_trabajo": "Situación laboral",
    "cod_ciuo": "Ocupación",
    "p44_lug_trab": "Lugar de trabajo",
    "p45_medio_transporte": "Medio de transporte"
}

SEXO_LABELS = {
    1: "Hombre",
    2: "Mujer"
}

P23_EST_CIVIL_LABELS = {
    1: "Casado/a",
    2: "Conviviente o pareja",
    3: "Conviviente civil (AUC)",
    4: "Anulado/a",
    5: "Separado/a",
    6: "Divorciado/a",
    7: "Viudo/a",
    8: "Soltero/a"
}

CINE11_LABELS = {
    1: "Nunca cursó educación",
    2: "Educación parvularia",
    3: "Básica incompleta",
    4: "Básica completa (6°)",
    5: "Básica completa (8°)",
    6: "Media científico-humanista",
    7: "Media técnico-profesional",
    8: "Técnico superior",
    9: "Universitaria",
    10: "Magíster/Especialización",
    11: "Doctorado",
    12: "Educación especial"
}

SIT_LABORAL_LABELS = {
    1: "Ocupado/a",
    2: "Desocupado/a",
    3: "Inactivo/a"
}

COD_CIUO_LABELS = {
    0: "Fuerzas armadas",
    1: "Directores y gerentes",
    2: "Profesionales científicos e intelectuales",
    3: "Técnicos y profesionales de nivel medio",
    4: "Personal de apoyo administrativo",
    5: "Trabajadores de servicios y vendedores",
    6: "Agricultores y trabajadores agropecuarios",
    7: "Artesanos y operarios",
    8: "Operadores de maquinaria",
    9: "Ocupaciones elementales",
    999: "No codificable"
}

P44_LUG_TRAB_LABELS = {
    1: "En mi vivienda (trabajo remoto)",
    2: "En mi comuna, fuera de mi vivienda",
    3: "En otra comuna",
    4: "En otro país",
    5: "En varias comunas o países"
}

P45_MEDIO_TRANSPORTE_LABELS = {
    1: "Auto particular",
    2: "Transporte público (bus, metro, taxi, colectivo)",
    3: "Caminando",
    4: "Bicicleta o scooter",
    5: "Motocicleta",
    6: "Caballo, lancha o bote",
    7: "Otro"
}

REGION_LABELS = {
    1: "Tarapacá",
    2: "Antofagasta",
    3: "Atacama",
    4: "Coquimbo",
    5: "Valparaíso",
    6: "O'Higgins",
    7: "Maule",
    8: "Biobío",
    9: "La Araucanía",
    10: "Los Lagos",
    11: "Aysén",
    12: "Magallanes",
    13: "Metropolitana",
    14: "Los Ríos",
    15: "Arica y Parinacota",
    16: "Ñuble"
}

EDAD_QUINQUENAL_LABELS = {
    0: "0-4 años",
    5: "5-9 años",
    10: "10-14 años",
    15: "15-19 años",
    20: "20-24 años",
    25: "25-29 años",
    30: "30-34 años",
    35: "35-39 años",
    40: "40-44 años",
    45: "45-49 años",
    50: "50-54 años",
    55: "55-59 años",
    60: "60-64 años",
    65: "65-69 años",
    70: "70-74 años",
    75: "75-79 años",
    80: "80-84 años",
    85: "85 años y más"
}


def get_valor_label(variable: str, valor: int) -> str:
    """Obtiene la etiqueta legible de un valor"""
    if valor is None:
        return "No especificado"
    
    if variable == "sexo":
        return SEXO_LABELS.get(valor, str(valor))
    elif variable == "p23_est_civil":
        return P23_EST_CIVIL_LABELS.get(valor, str(valor))
    elif variable == "cine11":
        return CINE11_LABELS.get(valor, str(valor))
    elif variable == "sit_fuerza_trabajo":
        return SIT_LABORAL_LABELS.get(valor, str(valor))
    elif variable == "cod_ciuo":
        return COD_CIUO_LABELS.get(valor, str(valor))
    elif variable == "p44_lug_trab":
        return P44_LUG_TRAB_LABELS.get(valor, str(valor))
    elif variable == "p45_medio_transporte":
        return P45_MEDIO_TRANSPORTE_LABELS.get(valor, str(valor))
    elif variable == "region":
        return REGION_LABELS.get(valor, str(valor))
    elif variable == "edad_quinquenal":
        return EDAD_QUINQUENAL_LABELS.get(valor, f"{valor}-{valor+4} años")
    elif variable == "comuna":
        return get_comuna_name(valor)
    else:
        return str(valor)


# ============================================
# ENDPOINTS
# ============================================

@app.get("/")
async def root():
    """Health check y estadísticas básicas"""
    return {
        "status": "ok",
        "mensaje": "API de Anonimización funcionando",
        "registros_cargados": len(df) if df is not None else 0,
        "version": "1.0.0"
    }


@app.get("/stats")
async def get_stats():
    """Estadísticas generales del dataset"""
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Datos no cargados")
    
    return {
        "total_registros": len(df),
        "total_regiones": df["region"].nunique(),
        "total_comunas": df["comuna"].nunique(),
        "memoria_mb": round(df.memory_usage(deep=True).sum() / 1024**2, 1)
    }


@app.get("/opciones")
async def get_opciones():
    """Retorna las opciones disponibles para el cuestionario"""
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Datos no cargados")
    
    # Regiones con labels
    regiones = []
    for r in sorted(df["region"].dropna().unique()):
        r_int = int(r)
        regiones.append({
            "value": r_int,
            "label": REGION_LABELS.get(r_int, f"Región {r_int}")
        })
    
    # Tramos de edad con labels
    edades = []
    for e in sorted(df["edad_quinquenal"].dropna().unique()):
        e_int = int(e)
        edades.append({
            "value": e_int,
            "label": EDAD_QUINQUENAL_LABELS.get(e_int, f"{e_int}-{e_int+4} años")
        })
    
    return {
        "regiones": regiones,
        "sexo": [
            {"value": k, "label": v} 
            for k, v in SEXO_LABELS.items()
        ],
        "edad_quinquenal": edades,
        "p23_est_civil": [
            {"value": k, "label": v} 
            for k, v in P23_EST_CIVIL_LABELS.items()
        ],
        "cine11": [
            {"value": k, "label": v}
            for k, v in CINE11_LABELS.items()
        ],
        "sit_fuerza_trabajo": [
            {"value": k, "label": v}
            for k, v in SIT_LABORAL_LABELS.items()
        ],
        "cod_ciuo": [
            {"value": k, "label": v}
            for k, v in COD_CIUO_LABELS.items()
            if k != 999  # Excluir "No codificable"
        ],
        "p44_lug_trab": [
            {"value": k, "label": v}
            for k, v in P44_LUG_TRAB_LABELS.items()
        ],
        "p45_medio_transporte": [
            {"value": k, "label": v}
            for k, v in P45_MEDIO_TRANSPORTE_LABELS.items()
        ]
    }


@app.get("/comunas/{region_id}")
async def get_comunas(region_id: int):
    """Retorna comunas de una región específica con nombres"""
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Datos no cargados")
    
    comunas = df[df["region"] == region_id]["comuna"].dropna().unique()
    # Retornar como lista de objetos con value y label
    resultado = []
    for c in sorted(comunas):
        c_int = int(c)
        resultado.append({
            "value": c_int,
            "label": get_comuna_name(c_int)
        })
    return resultado


@app.post("/buscar", response_model=ResultadoBusqueda)
async def buscar_perfil(perfil: PerfilBusqueda):
    """
    Busca cuántas personas coinciden con el perfil dado.
    Este es el endpoint principal de la demo.
    """
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Datos no cargados")
    
    # Construir filtro dinámicamente
    mask = pd.Series([True] * len(df))
    variables_usadas = []
    
    # Mapeo de campos del request a columnas del df
    filtros = [
        ("region", perfil.region),
        ("comuna", perfil.comuna),
        ("sexo", perfil.sexo),
        ("edad_quinquenal", perfil.edad_quinquenal),
        ("p23_est_civil", perfil.p23_est_civil),
        ("cine11", perfil.cine11),
        ("sit_fuerza_trabajo", perfil.sit_fuerza_trabajo),
        ("cod_ciuo", perfil.cod_ciuo),
        ("p44_lug_trab", perfil.p44_lug_trab),
        ("p45_medio_transporte", perfil.p45_medio_transporte),
    ]
    
    for col, valor in filtros:
        if valor is not None:
            mask = mask & (df[col] == valor)
            variables_usadas.append(col)
    
    n_coincidencias = mask.sum()
    total = len(df)
    porcentaje = (n_coincidencias / total) * 100
    
    # Determinar mensaje
    if n_coincidencias == 0:
        mensaje = "No hay nadie con ese perfil exacto en el censo. Podrías ser identificado por exclusión."
    elif n_coincidencias == 1:
        mensaje = "⚠️ ¡ERES ÚNICO! Con solo estos datos, serías completamente identificable."
    elif n_coincidencias < 10:
        mensaje = f"Casi único: solo {n_coincidencias} personas en Chile comparten tu perfil."
    elif n_coincidencias < 100:
        mensaje = f"Relativamente identificable: {n_coincidencias} personas comparten tu perfil."
    else:
        mensaje = f"Compartes tu perfil con {n_coincidencias:,} personas."
    
    return ResultadoBusqueda(
        n_coincidencias=int(n_coincidencias),
        total_poblacion=total,
        porcentaje=round(porcentaje, 6),
        k_anonimidad=int(n_coincidencias),
        es_unico=(n_coincidencias <= 1),
        mensaje=mensaje,
        variables_usadas=variables_usadas
    )


@app.post("/buscar-progresivo")
async def buscar_progresivo(perfil: PerfilBusqueda) -> list[ResultadoProgresivo]:
    """
    Muestra cómo disminuye el anonimato al agregar cada variable.
    Retorna el "embudo" de identificación.
    
    La lógica condicional:
    - cod_ciuo y p44_lug_trab solo se incluyen si sit_fuerza_trabajo = 1 (Ocupado)
    - p45_medio_transporte solo se incluye si p44_lug_trab IN (2,3,4,5)
    """
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Datos no cargados")
    
    resultados = []
    mask = pd.Series([True] * len(df))
    total = len(df)
    
    # Variables base (siempre se muestran)
    variables_base = [
        ("region", perfil.region),
        ("comuna", perfil.comuna),
        ("sexo", perfil.sexo),
        ("edad_quinquenal", perfil.edad_quinquenal),
        ("p23_est_civil", perfil.p23_est_civil),
        ("cine11", perfil.cine11),
        ("sit_fuerza_trabajo", perfil.sit_fuerza_trabajo),
    ]
    
    # Variables condicionales
    variables_condicionales = []
    
    # Solo agregar cod_ciuo y p44_lug_trab si está ocupado
    if perfil.sit_fuerza_trabajo == 1:
        variables_condicionales.append(("cod_ciuo", perfil.cod_ciuo))
        variables_condicionales.append(("p44_lug_trab", perfil.p44_lug_trab))
        
        # Solo agregar p45_medio_transporte si trabaja fuera de casa
        if perfil.p44_lug_trab in (2, 3, 4, 5):
            variables_condicionales.append(("p45_medio_transporte", perfil.p45_medio_transporte))
    
    variables_orden = variables_base + variables_condicionales
    
    # Resultado inicial (toda la población)
    resultados.append(ResultadoProgresivo(
        paso=0,
        variable="inicio",
        variable_label="Población total",
        valor=None,
        valor_label="Chile",
        n_coincidencias=total,
        porcentaje=100.0
    ))
    
    paso = 1
    for variable, valor in variables_orden:
        if valor is not None:
            mask = mask & (df[variable] == valor)
            n = mask.sum()
            
            resultados.append(ResultadoProgresivo(
                paso=paso,
                variable=variable,
                variable_label=VARIABLE_LABELS.get(variable, variable),
                valor=valor,
                valor_label=get_valor_label(variable, valor),
                n_coincidencias=int(n),
                porcentaje=round((n / total) * 100, 6)
            ))
            paso += 1
    
    return resultados


# ============================================
# PARA DESARROLLO LOCAL
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
