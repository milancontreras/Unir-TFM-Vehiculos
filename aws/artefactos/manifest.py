"""
Módulo para la generación del manifiesto de cada ejecución de ingesta.

Soporta persistencia local y en Amazon S3, según la configuración (USE_S3=true/false).
El manifiesto registra los artefactos descargados (por año), sus metadatos y la marca temporal
de ingesta. La salida se escribe bajo `metadata/<dataset>/manifest-<run_ts>.jsonl` (S3 o local).
"""

# Importaciones de la biblioteca estándar
from dataclasses import dataclass, asdict
from pathlib import Path
import json

# Importaciones para tipado estático
from typing import List, Dict, Any, Optional

# Importaciones de configuración local del proyecto
from config import OUT_META, USE_S3, S3_BUCKET
from storage.s3_paths import s3_key_manifest

import boto3

@dataclass
class ManifestEntry:
    """
    Entrada del manifiesto con los metadatos de un recurso descargado.

    Campos:
      - source: Fuente de datos (identificador lógico, p. ej., 'SRI').
      - resource_url: URL desde la cual se obtuvo el recurso.
      - metadata: Metadatos relevantes del recurso/dataset.
      - dataset: Nombre lógico del dataset.
      - year: Año asociado al recurso.
      - file_name: Nombre del archivo almacenado.
      - sha256: Hash de integridad del archivo (SHA‑256).
      - ingestion_ts: Marca temporal de la ingesta (ISO‑8601 o equivalente).
      - local_path: Ruta local de almacenamiento (si aplica).
      - notes: Observaciones del proceso (por defecto 'descarga_ok').
    """
    source: str
    resource_url: str
    metadata: Dict[str, Any]
    dataset: str
    year: int
    file_name: str
    sha256: str
    ingestion_ts: str
    local_path: str
    notes: str = "descarga_ok"

class ManifestWriter:
    """
    Acumula entradas y escribe el manifiesto de una ejecución.

    Parámetros:
      run_ts (str): Marca temporal que identifica la ejecución.

    Uso:
      writer = ManifestWriter(run_ts="20251103T120000Z")
      writer.add(entry)
      path_or_key = writer.write()
    """
    def __init__(self, run_ts: str):
        # Inicializa la ejecución con su marca temporal y una colección vacía de entradas.
        self.run_ts = run_ts
        self.entries: List[ManifestEntry] = []

    def add(self, entry: ManifestEntry):
        """Agrega una entrada al manifiesto en memoria."""
        # Inserta la entrada al final de la lista.
        self.entries.append(entry)

    def write(self) -> Optional[Path]:
        """Serializa y persiste el manifiesto (local o S3) para la ejecución actual."""
        # Si no hay entradas, no se realiza escritura.
        if not self.entries:
            print("INFO: No hay entradas nuevas en el manifest.")
            return None

        # Serializa las entradas en formato JSON Lines (NDJSON), una por línea.
        jsonl_str = "\n".join(json.dumps(asdict(e), ensure_ascii=False) for e in self.entries) + "\n"

        if USE_S3:
            # Publica el manifiesto en S3 usando boto3 y registra la ruta objetivo.
            key = s3_key_manifest(self.run_ts)
            s3_client = boto3.client("s3")
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=jsonl_str.encode("utf-8"),
                ContentType="application/json"
            )
            print(f"SUCCESS: Manifest subido a s3://{S3_BUCKET}/{key}")
            return key
        else:
            # Crea el directorio destino, escribe el archivo local y registra la ruta generada.
            OUT_META.mkdir(parents=True, exist_ok=True)
            path = OUT_META / f"manifest-{self.run_ts}.jsonl"
            with path.open("w", encoding="utf-8") as f:
                f.write(jsonl_str)
            print(f"SUCCESS: Manifest guardado localmente en {path}")
            return path