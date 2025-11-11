"""
Módulo de gestión del estado persistente del proceso de ingesta.

Mantiene un registro histórico del estado de ejecución anual, evitando descargas redundantes
y preservando información de control sobre cada ejecución. Soporta almacenamiento tanto
local como en Amazon S3, permitiendo operación híbrida entre entornos locales y en la nube.
"""

from datetime import datetime
import json

from typing import Dict, Optional

from config import STATE_PATH, OUT_META, USE_S3, S3_BUCKET
from storage.s3_paths import s3_key_state

import boto3
from botocore.exceptions import ClientError


def load_state() -> Dict:
    """
    Carga el estado actual del proceso de ingesta desde un archivo JSON.

    Si `USE_S3=True`, lee el archivo de estado desde el bucket configurado.
    Si `USE_S3=False`, lee el estado desde el sistema de archivos local.

    Maneja errores comunes como:
      - `NoSuchKey` o `NoSuchBucket`: devuelve un estado vacío sin interrumpir la ejecución.
      - Errores de lectura o parsing JSON: devuelve un estado vacío por seguridad.

    Returns:
        Dict: Diccionario con el estado actual o vacío si no existe información previa.
    """
    # Intenta cargar el estado desde S3 si la configuración lo permite.
    if USE_S3:
        s3 = boto3.client("s3")
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key_state())
            content = response['Body'].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ("NoSuchKey", "NoSuchBucket"):
                print(f"INFO: No se encontró el estado en S3 ({error_code}), retornando estado vacío.")
                return {}
            else:
                raise
    else:
        # Si no se usa S3, carga el estado desde el archivo local.
        # Verifica existencia del archivo local antes de intentar lectura.
        if STATE_PATH.exists():
            try:
                # Lee el contenido del archivo y lo convierte de JSON a dict
                return json.loads(STATE_PATH.read_text(encoding="utf-8"))
            except Exception:
                # Retorna un estado vacío si no existe archivo o si ocurrió un error.
                return {}
        # Retorna un estado vacío si no existe archivo o si ocurrió un error.
        return {}


def save_state(state: Dict) -> None:
    """
    Guarda el estado actual del proceso de ingesta en formato JSON.

    Si `USE_S3=True`, sube el estado al bucket configurado con tipo de contenido JSON.
    Si `USE_S3=False`, lo persiste localmente en el directorio de metadatos.

    Args:
        state (Dict): Diccionario con el estado actualizado a guardar.

    Efectos:
        - Crea directorios necesarios si no existen.
        - Genera el archivo `state.json` legible (indentado, UTF-8).
        - Reporta éxito en consola al completar la operación.
    """
    # Persiste el estado en S3 con codificación JSON.
    if USE_S3:
        s3 = boto3.client("s3")
        json_bytes = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key_state(),
            Body=json_bytes,
            ContentType="application/json"
        )
        print(f"SUCCESS: Estado actualizado en s3://{S3_BUCKET}/{s3_key_state()}")
    else:
        # Guarda el estado localmente asegurando la existencia de la carpeta destino.
        OUT_META.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def touch_checked(state: Dict, year: int, last_metadata_update: Optional[str], status: str, last_file_path: Optional[str] = None,last_sha256: Optional[str] = None,) -> None:
    """
    Actualiza o crea la entrada de estado correspondiente a un año específico.

    Registra la fecha del último chequeo (`last_checked_ts`) y los metadatos más recientes
    asociados a la operación. Esta función se invoca tanto al finalizar descargas como
    al verificar metadatos sin cambios.

    Args:
        state (Dict): Estado completo del proceso (estructura principal).
        year (int): Año de referencia a actualizar.
        last_metadata_update (Optional[str]): Fecha de última actualización detectada (si aplica).
        status (str): Estado textual de la última operación realizada.

    Estructura del registro:
        {
            'last_metadata_update': <fecha ISO o None>,
            'last_status': <texto descriptivo>,
            'last_checked_ts': <timestamp ISO-8601 actual>
        }
    """
    key = str(year)

    # Recupera la entrada existente o inicializa todas las claves estándar.
    yst = state.get(key, {
        "last_metadata_update": None,
        "last_status": "unknown",
        "last_checked_ts": None,
        "last_file_path": None,
        "last_sha256": None,
    })

    # Actualiza los valores principales.
    yst["last_metadata_update"] = last_metadata_update
    yst["last_status"] = status
    yst["last_checked_ts"] = datetime.now().isoformat()

    if last_file_path:
        yst["last_file_path"] = last_file_path
    if last_sha256:
        yst["last_sha256"] = last_sha256

    # Mantiene consistencia en el estado global.
    state[key] = yst