"""
Script principal para la ingesta incremental de datos de vehículos nuevos (SRI).

Este módulo implementa un flujo de extracción controlado por metadatos y estado persistente,
alineado con la arquitectura medallion (Bronze → Silver → Gold). Descarga archivos CSV por año,
decide su actualización según los metadatos del portal y mantiene un registro de ejecuciones
mediante un archivo de estado (state.json) y un manifiesto de descargas.

Artefactos generados:
- state.json: estado por año (última fecha de actualización conocida, hash, timestamps y estado).
- manifest (JSONL/JSON): listado de archivos descargados en la ejecución con metadatos asociados.

La capa Bronze conserva los CSV originales versionados y facilita la trazabilidad para
etapas posteriores de limpieza (Silver) y consumo analítico (Gold).
"""

# -----------------------------------------------------------------------------
# Importaciones
#   - argparse: parseo de argumentos para ejecución por CLI.
#   - os, pathlib, datetime, pytz: utilidades de sistema, rutas y fechas/zonas horarias.
#   - config: parámetros globales del proyecto (rutas, flags, años por defecto).
#   - utils: helpers HTTP, timestamps, hashing y deduplicación local.
#   - metadata: lectura de metadatos del portal para decidir descargas.
#   - downloader: descarga y persistencia de CSV (local o S3).
#   - state_manager: carga/actualización del estado incremental.
#   - manifest: escritura del manifiesto de descargas por ejecución.
# -----------------------------------------------------------------------------

import argparse
import os
from datetime import datetime
from pathlib import Path
import pytz
import json

from config import (
    DEFAULT_START, DEFAULT_END, OUT_RAW, OUT_META, DATASET_PREFIX, USE_S3, S3_BUCKET, FORCE_DEFAULT,
    DATABRICKS_HOST, DATABRICKS_TOKEN, JOB_ID
)
from utils import get_session, now_ts, existing_year_hashes, sha256_bytes
from metadata import scrape_metadatos_por_anio
from downloader import download_csv, save_csv_local, save_csv_s3
from state_manager import load_state, save_state, touch_checked
from manifest import ManifestWriter, ManifestEntry
from databricks_trigger import trigger_databricks_job

def decide_download(force: bool, last_seen_update: str | None, current_meta_update: str | None) -> tuple[bool, str]:
    """
    Determina si corresponde descargar el CSV para un año dado.

    Criterios considerados:
    - `force=True`: fuerza la descarga independientemente del estado previo.
    - Falta de estado y metadatos: se descarga por precaución.
    - Primer registro del año (sin estado previo): se descarga.
    - Cambio en `current_meta_update` respecto a `last_seen_update`: se descarga.
    - Sin cambios detectados: se omite la descarga.

    Args:
        force (bool): Fuerza la descarga si es True.
        last_seen_update (str | None): Fecha de última actualización conocida en el estado.
        current_meta_update (str | None): Fecha de última actualización reportada por metadatos.

    Returns:
        tuple[bool, str]: (debe_descargar, razón de la decisión).
    """
    if force:
        # Forzar descarga sin importar estado previo
        return True, "--force"
    if last_seen_update is None and current_meta_update is None:
        # No hay fecha en web ni en estado local, se descarga por precaución
        return True, "sin_fecha_en_web_y_sin_estado"
    if last_seen_update is None:
        # Primer registro para este año, descargar
        return True, "primer_registro_del_anio"
    if current_meta_update and current_meta_update != last_seen_update:
        # Fecha de actualización cambió, descargar nuevo CSV
        return True, f"actualizacion_detectada: {last_seen_update} -> {current_meta_update}"
    # No hay cambios detectados, no descargar
    return False, "sin_cambios"

def main(start: int, end: int, force: bool, metadata_only: bool = False):
    """
    Ejecuta el flujo de ingesta incremental para un rango de años.

    Pasos principales:
    1) Preparar salidas (local o S3).
    2) Cargar estado previo (state.json).
    3) Inicializar sesión HTTP y el escritor de manifiesto.
    4) Iterar por año:
       a) Obtener metadatos.
       b) Si `metadata_only=True`, registrar chequeo y continuar.
       c) Decidir descarga según metadatos/estado.
       d) Si no corresponde, registrar chequeo y continuar.
       e) Descargar CSV con manejo de errores.
       f) Deduplicar por SHA‑256.
       g) Guardar CSV versionado por `ingestion_ts` (local/S3).
       h) Registrar la entrada en el manifiesto.
       i) Actualizar estado por año y persistir.
    5) Escribir el manifiesto de la ejecución.

    Efectos:
    - CSV guardados en `OUT_RAW/year=YYYY/<archivo>__ingest_ts=<TS>.csv` (o S3 equivalente).
    - `state.json` actualizado con la última evidencia por año.
    - Manifiesto con el detalle de descargas realizadas.

    Notas:
    - Requiere configuración previa en `config.py`.
    - `metadata_only=True` permite ejecutar solo la verificación de metadatos.
    """
    # 1) Preparar estructura de salida (solo en modo local; en S3 no se requiere).
    if not USE_S3:
        OUT_RAW.mkdir(parents=True, exist_ok=True)
        OUT_META.mkdir(parents=True, exist_ok=True)

    # 2) Cargar estado incremental previo desde state.json (local o S3, según configuración).
    state = load_state()
    # 3) Inicializar sesión HTTP y escritor de manifiesto para esta ejecución.
    session = get_session()
    run_ts = now_ts()  # Timestamp de ejecución para versionado
    mw = ManifestWriter(run_ts)  # Gestor de manifest para esta ejecución
    total_nuevos = 0  # Contador de nuevos archivos descargados

    # 4) Procesar cada año del rango solicitado.
    for year in range(start, end + 1):
        print(f"INFO: Procesando año {year}")

        # a) Obtener metadatos del dataset para el año actual.
        try:
            meta = scrape_metadatos_por_anio(year, session)
            meta_update_iso = meta.get("fecha_actualizacion")  # Fecha ISO de última actualización
        except Exception as e:
            print(f"ERROR: No se pudieron leer los metadatos para el año {year}: {e}")
            meta = {"error": str(e), "metadata_page": f"(year={year})"}
            meta_update_iso = None
            # Se registra el error en metadatos y se continúa con lógica defensiva.

        # b) Si `metadata_only=True`, registrar el chequeo de metadatos y continuar sin descargar.
        if metadata_only:
            yst = state.get(str(year), {})
            yst["last_metadata_update"] = meta_update_iso
            state[str(year)] = yst
            touch_checked(state, year, meta_update_iso, status="meta_only_checked")
            save_state(state)
            print(f"INFO: Modo metadata-only activo; se registró el chequeo y se omite la descarga para el año {year}.")
            continue

        # c) Decidir descarga comparando estado previo vs. metadatos actuales.
        yst = state.get(str(year), {})
        last_seen = yst.get("last_metadata_update")
        need_download, reason = decide_download(force, last_seen, meta_update_iso)
        print(f"INFO: Decisión para año {year}: {'DESCARGAR' if need_download else 'OMITIR'} ({reason})")

        # d) Registrar el chequeo de metadatos aunque no haya descarga y persistir estado.
        if not need_download:
            touch_checked(state, year, meta_update_iso, status="no_changes")
            save_state(state)
            continue

        # e) Descargar CSV (manejo de 404 y errores transitorios).
        try:
            content, filename, csv_url = download_csv(year, session)
        except FileNotFoundError as e404:
            print(f"ERROR: {e404}")
            touch_checked(state, year, meta_update_iso, status="404")
            save_state(state)
            continue
        except Exception as e:
            print(f"ERROR: Error al descargar el archivo CSV del año {year}: {e}")
            touch_checked(state, year, meta_update_iso, status="error_download")
            save_state(state)
            continue

        # f) Deduplicar contenido mediante hash SHA‑256 (evita duplicados locales/S3).
        digest = sha256_bytes(content)  # Hash SHA-256 del contenido CSV
        is_duplicate = False
        if USE_S3:
            # En modo S3, deduplicamos usando el último hash guardado en el estado
            last_sha = (state.get(str(year), {}) or {}).get("last_sha256")
            if last_sha and last_sha == digest:
                is_duplicate = True
        else:
            # En modo local, deduplicamos comparando contra hashes de archivos existentes en disco
            year_root = OUT_RAW / f"year={year}"
            if digest in existing_year_hashes(year_root):
                is_duplicate = True

        # Si el hash coincide con el último conocido (S3) o existente (local), se omite la persistencia.
        if is_duplicate:
            print(f"WARNING: El archivo del año {year} tiene el mismo contenido (hash coincidente). No se guarda nuevamente.")
            touch_checked(state, year, meta_update_iso, status="200_same")
            save_state(state)
            continue

        # g) Guardar CSV con sufijo `__ingest_ts=<run_ts>` dentro de `year=YYYY` (local o S3).
        base_name, ext = os.path.splitext(filename)
        filename = f"{base_name}__ingest_ts={run_ts}.csv"
        if USE_S3:
            dest_path_str = f"s3://{S3_BUCKET}/" + save_csv_s3(content, year, filename)
        else:
            dest_dir = OUT_RAW / f"year={year}"
            dest_path = save_csv_local(content, dest_dir, filename)
            dest_path_str = str(dest_path)

        # h) Registrar en el manifiesto: metadatos, hash, destino y marca temporal de ingesta.
        ecuador_tz = pytz.timezone("America/Guayaquil")
        now_local = datetime.now(ecuador_tz)

        # Generar timestamp ISO‑8601 (zona America/Guayaquil) para traza de ingesta.
        iso_str = now_local.isoformat()

        mw.add(ManifestEntry(
            source="datosabiertos.gob.ec",
            resource_url=csv_url,
            metadata=meta,
            dataset=DATASET_PREFIX,
            year=year,
            file_name=filename,
            sha256=digest,
            ingestion_ts=iso_str,
            local_path=dest_path_str,
            notes="descarga_ok"
        ))
        total_nuevos += 1
        print(f"SUCCESS: Archivo {filename} guardado correctamente. SHA-256: {digest[:12]}...")

        # i) Actualizar y persistir el estado del año (última fecha de metadatos, hash y `ingestion_ts`).
        yst = state.get(str(year), {})
        yst["last_metadata_update"] = meta_update_iso
        yst["last_sha256"] = digest
        yst["last_status"] = "200_new"
        yst["last_file_path"] = dest_path_str
        yst["last_sha256"] = digest
        touch_checked(state, year, meta_update_iso, status=yst["last_status"], last_file_path=dest_path_str,last_sha256=digest,)
        save_state(state)

    # 5) Escribir el manifiesto de la ejecución y reportar ruta resultante.
    manifest_path = mw.write()
    if manifest_path:
        print(f"INFO: Manifest generado en: {manifest_path}")
    else:
        print("INFO: No se registraron nuevos archivos en esta ejecución.")

    print(f"INFO: Total de archivos nuevos descargados: {total_nuevos}")

    # 6) Levantar el trigger para que se ejecute el job en databricks
    try:
        result = trigger_databricks_job(DATABRICKS_HOST, DATABRICKS_TOKEN, JOB_ID)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Trigger Job Databricks lanzado exitosamente (jobId:{JOB_ID})",
                "databricks_response": result
            })
        }

    except Exception as e:
        print(f"Error en el trigger de Databricks (jobId:{JOB_ID})", str(e))

        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Error al lanzar el trigger de Databricks job",
                "details": str(e)
            })
        }

# --- Handler AWS Lambda ---
def lambda_handler(event, context):
    """
    Punto de entrada para AWS Lambda.

    Lee parámetros del evento/entorno, determina el rango de años a procesar
    (cerrando en el año actual) y ejecuta el flujo principal. Reporta parámetros de
    ejecución y resultado básico.
    """

    anio_actual = int(now_ts()[:4])
    # Año actual derivado de la marca temporal local (America/Guayaquil).

    # Parámetros de entrada: se permite override vía evento o variables de entorno.
    start = int(event.get("start", os.getenv("DEFAULT_START", DEFAULT_START)))
    if (anio_actual is None) or (anio_actual == ""):
        end = int(event.get("end", os.getenv("DEFAULT_END", DEFAULT_END)))
    else:
        end=anio_actual

    # Flags de comportamiento: forzar descarga y modo solo metadatos.
    force = event.get("force", os.getenv("FORCE", str(FORCE_DEFAULT)).lower() == "true")
    metadata_only = event.get("metadata_only", os.getenv("METADATA_ONLY", "false")).__str__().lower() == "true"

    print(f"INFO: Lambda handler -> start={start}, end={end}, force={force}, metadata_only={metadata_only}")
    main(start, end, force, metadata_only=metadata_only)
    return {"status": "ok", "start": start, "end": end, "force": force, "metadata_only": metadata_only}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta incremental SRI vehículos (modo CLI).")
    parser.add_argument("--start", type=int, default=DEFAULT_START, help="Año inicial (inclusive)")
    parser.add_argument("--end", type=int, default=DEFAULT_END, help="Año final (inclusive)")
    parser.add_argument("--force", action="store_true", help="Forzar descarga aunque no haya cambios")
    parser.add_argument("--metadata-only", action="store_true", help="Solo consultar metadatos sin descargar archivos")
    args = parser.parse_args()
    main(args.start, args.end, args.force, metadata_only=args.metadata_only)
