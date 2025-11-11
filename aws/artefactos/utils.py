"""
Módulo de utilidades generales para el flujo de ingesta y validación de datos.

Incluye funciones para:
- Cálculo de hashes SHA‑256.
- Parseo de fechas en formato español.
- Configuración de sesiones HTTP con cabeceras personalizadas.
- Generación de timestamps en zona horaria de Ecuador.
- Verificación de duplicados mediante hashes de archivos.
"""

# Importaciones de librerías estándar
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Set
import pytz

# Importaciones de librerías de terceros
import requests

# Importaciones locales
from config import USER_AGENT

MESES_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

def sha256_bytes(b: bytes) -> str:
    """
    Calcula el hash SHA‑256 de un contenido binario.

    Args:
        b (bytes): Contenido binario a procesar.

    Returns:
        str: Representación hexadecimal del hash SHA‑256.
    """
    # Inicializa el objeto hash.
    h = hashlib.sha256()
    # Actualiza el hash con los bytes proporcionados.
    h.update(b)
    # Devuelve el digest en formato hexadecimal.
    return h.hexdigest()

def parse_fecha_es(texto: Optional[str]) -> Optional[str]:
    """
    Convierte una fecha escrita en español al formato ISO (YYYY‑MM‑DD).

    Soporta expresiones del tipo: '6 de abril de 2023'.

    Args:
        texto (Optional[str]): Cadena de texto con la fecha en español.

    Returns:
        Optional[str]: Fecha en formato ISO o None si no se puede interpretar.
    """
    if not texto:
        return None
    # Normaliza espacios y convierte la cadena a minúsculas.
    t = re.sub(r"\s+", " ", texto.strip().lower())
    # Busca el patrón 'día de mes de año' en el texto normalizado.
    m = re.search(r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})", t)
    if not m:
        return None
    # Extraer día
    d = int(m.group(1))
    # Normaliza el nombre del mes eliminando tildes.
    mes_txt = (m.group(2)
               .replace("á","a").replace("é","e")
               .replace("í","i").replace("ó","o").replace("ú","u"))
    # Extraer año
    y = int(m.group(3))
    # Busca el número de mes correspondiente en el diccionario.
    mes = MESES_ES.get(mes_txt)
    if not mes:
        return None
    # Retorna la fecha convertida en formato ISO.
    return f"{y:04d}-{mes:02d}-{d:02d}"

def get_session() -> requests.Session:
    """
    Crea y devuelve una sesión HTTP preconfigurada.

    Configura un encabezado 'User‑Agent' definido en la variable de entorno `USER_AGENT`.

    Returns:
        requests.Session: Sesión HTTP con cabeceras predefinidas.
    """
    # Inicializa una sesión HTTP reutilizable.
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    # Retorna la sesión configurada.
    return s

def now_ts() -> str:
    """
    Genera una marca temporal (timestamp) de la hora actual en la zona horaria de Ecuador.

    Formato: 'YYYYMMDD_HHMMSS'

    Returns:
        str: Timestamp formateado según la hora local.
    """
    # Obtiene la zona horaria local de Ecuador.
    ecuador_tz = pytz.timezone("America/Guayaquil")
    now_local = datetime.now(ecuador_tz)

    # Formatea la hora local en el formato especificado.
    return datetime.now(ecuador_tz).strftime("%Y%m%d_%H%M%S")

def existing_year_hashes(root_year_dir: Path) -> Set[str]:
    """
    Calcula los hashes SHA‑256 de todos los archivos CSV en un directorio de año.

    Args:
        root_year_dir (Path): Ruta al directorio raíz del año (por ejemplo, 'year=2023/').

    Returns:
        Set[str]: Conjunto de hashes SHA‑256 de los archivos CSV encontrados.
    """
    # Inicia el cálculo de hashes para los archivos existentes.
    print("INFO: Cálculo de hashes SHA-256 de archivos existentes...")
    # Si no existe el directorio, devuelve un conjunto vacío.
    if not root_year_dir.exists():
        return set()
    digests = set()
    # Recorre recursivamente todos los archivos CSV en el directorio.
    for f in root_year_dir.rglob("*.csv"):
        try:
            # Calcula y agrega el hash SHA‑256 de cada archivo.
            digests.add(sha256_bytes(f.read_bytes()))
        except Exception:
            pass
    # Devuelve el conjunto de hashes calculados.
    return digests