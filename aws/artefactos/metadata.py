"""
Módulo responsable de extraer metadatos del portal Datos Abiertos del Ecuador.

Obtiene información de fuente, autor, contacto, fechas y licencia de uso para cada año del dataset.
Prioriza la obtención vía API CKAN y, si no está disponible, recurre al análisis de HTML con BeautifulSoup.
Retorna un diccionario normalizado con las claves requeridas por el flujo de ingesta.
"""

from typing import Dict, Optional
from typing import Any, List

from bs4 import BeautifulSoup
import requests

from config import BASE_META,BASE_META_2, TIMEOUT
from utils import parse_fecha_es


def _get_value_after_label(soup: BeautifulSoup, label: str) -> Optional[str]:
    """
    Devuelve el texto del párrafo siguiente a una etiqueta específica dentro del HTML.

    Args:
        soup (BeautifulSoup): Documento HTML ya parseado.
        label (str): Etiqueta a buscar (por ejemplo: 'Fuente', 'Autor').

    Returns:
        Optional[str]: Texto del párrafo hermano siguiente, o None si no se encuentra.
    """
    # Itera sobre todos los párrafos para encontrar el que coincida exactamente con la etiqueta dada
    for p in soup.find_all("p"):
        if p.get_text(strip=True).lower() == label.lower():
            # Obtiene el siguiente párrafo hermano que contiene el valor deseado
            sib = p.find_next_sibling("p")
            if sib:
                return sib.get_text(" ", strip=True)
    # Retorna None si no se encontró la etiqueta o el valor asociado
    return None

def select_csv_resource(resources: List[Dict[str, Any]], year: int) -> Optional[Dict[str, Any]]:
    """
    Selecciona el recurso CSV más probable dentro de la lista de recursos del dataset.

    La heurística prioriza el formato 'CSV' y, como respaldo, verifica coincidencia de nombre con el año.

    Args:
        resources (list[dict]): Lista de recursos provistos por CKAN.
        year (int): Año del dataset a filtrar.

    Returns:
        Optional[dict]: Recurso seleccionado o None si no se encuentra.
    """
    for r in resources or []:
        fmt = (r.get("format") or "").upper()
        name = (r.get("name") or "").lower()
        if fmt == "CSV" or ("csv" in fmt) or (str(year) in name and "csv" in name):
            return r
    return None

def fetch_api_metadata(year: int, session: requests.Session) -> Optional[Dict]:
    """
    Intenta obtener metadatos del dataset para el año dado utilizando la API CKAN.

    Fuente consultada: /package_search?q=<slug>, donde <slug> = 'estadisticas-vehiculos-<year>'.
    Si el dataset existe, normaliza y retorna únicamente las claves esperadas por el pipeline.

    Args:
        year (int): Año del dataset.
        session (requests.Session): Sesión HTTP reutilizable.

    Returns:
        Optional[Dict]: Diccionario con las claves:
            - metadata_page
            - fuente
            - autor
            - contacto_email
            - fecha_actualizacion
            - fecha_creacion
            - licencia_nombre
            - licencia_url
            - archivo_url
        None si la consulta falla o no se encuentran resultados.
    """
    # Construye el identificador del dataset y la URL de búsqueda en CKAN.
    dataset_id = f"estadisticas-vehiculos-{year}"

    url_search = f"{BASE_META_2}/package_search?q={dataset_id}"
    # Ejecuta la consulta a CKAN y valida la respuesta.
    try:
        r = session.get(url_search, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            return None
        results = data.get("result", {}).get("results", [])
        dataset = None
        for d in results:
            if d.get("name") == dataset_id:
                dataset = d
                break
        if dataset is None:
            return None
    except (requests.HTTPError, requests.RequestException):
        return None
    if not dataset:
        return None

    # Normaliza los campos relevantes desde el objeto 'dataset'.
    metadata_page = f"https://datosabiertos.gob.ec/dataset/{dataset_id}"
    org = dataset.get("organization") or {}
    fuente = org.get("title") or dataset.get("author")
    autor = dataset.get("author")
    contacto_email = dataset.get("author_email")
    licencia_nombre = dataset.get("license_title")
    licencia_url = dataset.get("license_url")
    fecha_creacion_iso = dataset.get("metadata_created")

    # Obtiene el recurso CSV y sus campos asociados (URL y última modificación).
    resources = dataset.get("resources", [])
    resource = resources[0]
    if resource:
        archivo_url = resource.get("url")
        fecha_actualizacion_iso = resource.get("metadata_modified")
    else:
        archivo_url = None
        fecha_actualizacion_iso = None

    return {
        "metadata_page": metadata_page,
        "fuente": fuente,
        "autor": autor,
        "contacto_email": contacto_email,
        "fecha_actualizacion": fecha_actualizacion_iso,
        "fecha_creacion": fecha_creacion_iso,
        "licencia_nombre": licencia_nombre,
        "licencia_url": licencia_url,
        "archivo_url": archivo_url
    }

def scrape_metadatos_por_url(year: int, session: requests.Session) -> Dict:
    """
    Extrae metadatos desde la página HTML del dataset para el año indicado.

    Utiliza BeautifulSoup para localizar etiquetas estándar (Fuente, Autor, Correo,
    Fecha de actualización y Fecha de creación) y para identificar la licencia de uso.

    Args:
        year (int): Año del conjunto de datos.
        session (requests.Session): Sesión HTTP reutilizable.

    Returns:
        Dict: Diccionario con las claves:
            - metadata_page
            - fuente
            - autor
            - contacto_email
            - fecha_actualizacion
            - fecha_creacion
            - licencia_nombre
            - licencia_url
            - archivo_url
    """
    
    # Construye la URL pública de metadatos para el año solicitado.
    url_metadata = BASE_META.format(year=year)
    try:
        # Realiza la petición HTTP y valida el estado.
        r = session.get(url_metadata, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")       
        if soup is None or soup == "":
            return None
    except (requests.HTTPError, requests.RequestException):
        return None
    
    # Realiza la petición HTTP para obtener el HTML de la página de metadatos
    r = session.get(url_metadata, timeout=TIMEOUT)
    r.raise_for_status()
    # Parsea el contenido HTML con BeautifulSoup
    soup = BeautifulSoup(r.text, "html.parser")

    # Extrae 'Fuente' y 'Autor' desde el bloque de metadatos.
    fuente = _get_value_after_label(soup, "Fuente")
    autor  = _get_value_after_label(soup, "Autor")

    # Localiza un enlace de correo electrónico si está presente.
    mail = None
    a_mail = soup.select_one('a[href^="mailto:"]')
    if a_mail:
        mail = a_mail.get_text(strip=True)

    # Extrae y parsea fechas de actualización y creación.
    f_act_txt = _get_value_after_label(soup, "Fecha de actualización")
    f_cre_txt = _get_value_after_label(soup, "Fecha de creación")
    f_act = parse_fecha_es(f_act_txt) if f_act_txt else None
    f_cre = parse_fecha_es(f_cre_txt) if f_cre_txt else None

    # Identifica el bloque de 'Licencia de uso' y extrae su nombre y URL.
    licencia_nombre, licencia_url = None, None
    lic_block = None
    # Busca el bloque de licencia en los párrafos
    for p in soup.find_all("p"):
        if p.get_text(strip=True).lower() == "licencia de uso":
            lic_block = p.find_next_sibling("p")
            break
    # Extrae nombre y URL de la licencia si están disponibles
    if lic_block:
        a_rights = lic_block.find("a", attrs={"rel": "dc:rights"})
        if a_rights:
            licencia_nombre = a_rights.get_text(strip=True)
            licencia_url = a_rights.get("href")
        else:
            a_any = lic_block.find("a")
            if a_any:
                licencia_nombre = a_any.get_text(strip=True)
                licencia_url = a_any.get("href")

    # Devuelve el diccionario normalizado con los metadatos requeridos.
    return {
        "metadata_page": url_metadata,
        "fuente": fuente,
        "autor": autor,
        "contacto_email": mail,
        "fecha_actualizacion": f_act,
        "fecha_creacion": f_cre,
        "licencia_nombre": licencia_nombre,
        "licencia_url": licencia_url,
        "archivo_url": f"http://descargas.sri.gob.ec/download/datosAbiertos/SRI_Vehiculos_Nuevos_{year}.csv'"
    }

def scrape_metadatos_por_anio(year: int, session: requests.Session) -> Dict:
    """
    Orquesta la lectura de metadatos priorizando CKAN y usando HTML como respaldo.

    Intenta primero la API CKAN y, si no hay datos o falla la consulta, utiliza el scraper HTML.
    Retorna siempre el conjunto mínimo de claves necesarias para el pipeline.

    Args:
        year (int): Año del dataset.
        session (requests.Session): Sesión HTTP reutilizable.

    Returns:
        Dict: Metadatos con las claves:
            - metadata_page
            - fuente
            - autor
            - contacto_email
            - fecha_actualizacion
            - fecha_creacion
            - licencia_nombre
            - licencia_url
            - archivo_url
    """
    result = fetch_api_metadata(year, session)
    if result:
        return result
    
    result = scrape_metadatos_por_url(year, session)
    return result