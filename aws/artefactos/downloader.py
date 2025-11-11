"""
Módulo responsable de la descarga y almacenamiento de archivos CSV provenientes del Servicio de Rentas Internas (SRI).

Este componente forma parte del flujo de ingestión de datos y permite obtener los archivos fuente (raw)
correspondientes a un año determinado. Los archivos pueden almacenarse localmente o en Amazon S3 según la configuración del proyecto.
"""

# Librerías estándar
from pathlib import Path
from typing import Tuple

# Dependencias externas
import requests
import boto3

# Módulos locales del proyecto
from config import BASE_CSV, TIMEOUT, USE_S3, S3_BUCKET, S3_PREFIX
from storage.s3_paths import s3_key_raw

def download_csv(year: int, session: requests.Session) -> Tuple[bytes, str, str]:
    """
    Descarga el archivo CSV correspondiente a un año específico desde el SRI.

    Parámetros:
    - year (int): Año para el cual se desea descargar el archivo CSV.
    - session (requests.Session): Sesión HTTP para realizar la solicitud.

    Retorna:
    - Tuple[bytes, str, str]: Contenido del archivo en bytes, nombre del archivo y URL de descarga.

    Excepciones:
    - FileNotFoundError: Si la respuesta HTTP es 404, indicando que no existe el archivo para el año solicitado.
    - requests.HTTPError: Para otros errores HTTP ocurridos durante la descarga.
    """
    # Construye la URL de descarga para el año especificado.
    # Realiza la solicitud HTTP GET con un tiempo de espera definido.
    # Si el recurso no existe (404), lanza una excepción específica.
    # Verifica y lanza errores HTTP para otros códigos de estado.
    # Define un nombre estándar para el archivo descargado.
    # Retorna el contenido en bytes, el nombre del archivo y la URL de descarga.
    url = BASE_CSV.format(year=year)
    resp = session.get(url, timeout=TIMEOUT, stream=True)
    if resp.status_code == 404:
        raise FileNotFoundError(f"No existe CSV para {year} (404).")
    resp.raise_for_status()
    filename = f"SRI_Vehiculos_Nuevos_{year}.csv"
    return resp.content, filename, url

def save_csv_local(content: bytes, out_dir: Path, filename: str) -> Path:
    """
    Guarda el contenido CSV en un directorio local versionado.

    Parámetros:
    - content (bytes): Contenido del archivo CSV en bytes.
    - out_dir (Path): Directorio donde se guardará el archivo CSV.
    - filename (str): Nombre con el que se guardará el archivo.

    Retorna:
    - Path: Ruta completa al archivo CSV guardado localmente.
    """
    # Crea el directorio destino si no existe.
    # Construye la ruta completa donde se almacenará el archivo.
    # Escribe el contenido binario del CSV en el archivo especificado.
    # Retorna la ruta local del archivo guardado.
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / filename
    dest.write_bytes(content)
    return dest

def save_csv_s3(content: bytes, year: int, filename: str) -> str:
    """
    Guarda el contenido CSV en un bucket S3 bajo una clave construida a partir del año y nombre del archivo.

    Parámetros:
    - content (bytes): Contenido del archivo CSV en bytes.
    - year (int): Año asociado al archivo CSV.
    - filename (str): Nombre con el que se guardará el archivo en S3.

    Retorna:
    - str: La clave S3 donde se guardó el archivo.
    """
    # Construye la clave de almacenamiento S3 con base en el año y el nombre del archivo.
    # Inicializa el cliente S3 y carga el archivo al bucket configurado.
    # Registra en consola el resultado exitoso de la carga.
    # Retorna la clave S3 completa donde se guardó el archivo.
    key = s3_key_raw(year, filename)
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=content)
    print(f"SUCCESS: Archivo {filename} subido a s3://{S3_BUCKET}/{key}")
    return key