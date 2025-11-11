"""
Configuración general del proyecto de ingesta de datos del TFM.

Este módulo centraliza las constantes, rutas y parámetros de conexión utilizados
tanto en ejecución local como en la nube (AWS Lambda o EC2). Permite alternar entre
almacenamiento local y S3 mediante la variable USE_S3.
"""

import os
from pathlib import Path

# -----------------------------------------------------------------------------
# URLs base de los datasets del SRI (fuentes oficiales de datos abiertos)
# -----------------------------------------------------------------------------
BASE_CSV = os.getenv("BASE_CSV","https://descargas.sri.gob.ec/download/datosAbiertos/SRI_Vehiculos_Nuevos_{year}.csv")
BASE_META = os.getenv("BASE_META","https://datosabiertos.gob.ec/dataset/estadisticas-vehiculos-{year}")
BASE_META_2 = os.getenv("BASE_META_2","https://datosabiertos.gob.ec/api/3/action")

# -----------------------------------------------------------------------------
# Configuración de almacenamiento en S3 o local
# -----------------------------------------------------------------------------
# Si USE_S3=True, los archivos se almacenan directamente en el bucket configurado.
USE_S3 = True  # Ejecuta local pero sube todo directamente a S3

# Nombre del bucket S3 y parámetros regionales
S3_BUCKET = os.getenv("S3_BUCKET", "tfm-s3-datalake-12345unir")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip().strip("/")  # vacío = raíz del bucket
REGION = os.getenv("AWS_REGION", "us-east-1")

# Prefijo base para las subcarpetas (dataset)
DATASET_PREFIX = os.getenv("DATASET_PREFIX", "vehiculos") 

# -----------------------------------------------------------------------------
# Configuración de rutas locales (solo aplicable si USE_S3=False o para archivos temporales)
# -----------------------------------------------------------------------------
BASE_LOCAL = Path("output")  # en Lambda solo /tmp es writable
OUT_RAW = BASE_LOCAL / "raw" / DATASET_PREFIX
OUT_META = BASE_LOCAL / "metadata" / DATASET_PREFIX
STATE_PATH = OUT_META / "state.json"

# -----------------------------------------------------------------------------
# Configuración de peticiones HTTP y parámetros de metadatos
# -----------------------------------------------------------------------------
USER_AGENT = os.getenv("USER_AGENT", "MilanTFM-Scraper/1.0") 
TIMEOUT = int(os.getenv("TIMEOUT",40))

# -----------------------------------------------------------------------------
# Rango de años por defecto del dataset (ajustable por variables de entorno)
# -----------------------------------------------------------------------------
DEFAULT_START = int(os.getenv("DEFAULT_START", 2017))
DEFAULT_END = int(os.getenv("DEFAULT_END", 2025))
FORCE_DEFAULT = os.getenv("FORCE_DEFAULT", "false").lower() == "true"