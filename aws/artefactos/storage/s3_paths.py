from config import S3_PREFIX, DATASET_PREFIX

def _root(*parts: str) -> str:
    # Construye una ruta concatenando los segmentos proporcionados, respetando el prefijo definido en S3_PREFIX.
    base = S3_PREFIX
    path = "/".join(p.strip("/") for p in parts if p and p.strip("/") != "")
    return path if not base else f"{base}/{path}"

def s3_key_raw(year: int, filename: str) -> str:
    # Genera la ruta S3 para almacenar archivos en la zona 'raw' del data lake.
    # Estructura: raw/<dataset>/year=YYYY/<filename>
    return _root("raw", DATASET_PREFIX, f"year={year}", filename)

def s3_key_state() -> str:
    # Retorna la ruta S3 del archivo de estado (state.json) dentro de la carpeta de metadatos del dataset.
    return _root("metadata", DATASET_PREFIX, "state.json")

def s3_key_manifest(run_ts: str) -> str:
    # Retorna la ruta S3 del archivo manifest asociado a una ejecución específica (identificada por run_ts).
    # Estructura: metadata/<dataset>/manifest-<RUN_TS>.jsonl
    return _root("metadata", DATASET_PREFIX, f"manifest-{run_ts}.jsonl")
