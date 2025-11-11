#!/usr/bin/env bash
set -euo pipefail

# ===============================================================
# Script: upload_artifacts.sh
# Descripción:
#   Crea un bucket S3 (si no existe) y sube los artefactos del proyecto:
#     - extraer.zip → lambda/
#     - python.zip  → layers/
#     - template.yml → plantillas/
#
# Uso:
#   ./upload_artifacts.sh <bucket-name> [region]
#
# Ejemplo:
#   ./upload_artifacts.sh tfm-artifacts-2025 eu-west-2
#
# ===============================================================

# --- Variables de entrada ---
BUCKET_NAME=${1:-}
REGION=${2:-"eu-west-2"}

# --- Validación de entrada ---
if [[ -z "$BUCKET_NAME" ]]; then
  echo "[ERROR] Debes especificar el nombre del bucket."
  echo "Uso: $0 <bucket-name> [region]"
  exit 1
fi

# --- Verificación de archivos requeridos ---
for f in extraer.zip python.zip template.yml; do
  if [[ ! -f "$f" ]]; then
    echo "[ERROR] No se encontró el archivo requerido: $f"
    exit 1
  fi
done

echo "[INFO] Región seleccionada: ${REGION}"
echo "[INFO] Bucket destino: ${BUCKET_NAME}"

# ===============================================================
# 1. Crear bucket (si no existe)
# ===============================================================
if aws s3api head-bucket --bucket "${BUCKET_NAME}" 2>/dev/null; then
  echo "[INFO] El bucket ${BUCKET_NAME} ya existe. Continuando..."
else
  echo "[INFO] Creando bucket S3: ${BUCKET_NAME} en ${REGION}..."
  aws s3api create-bucket \
    --bucket "${BUCKET_NAME}" \
    --region "${REGION}" \
    --create-bucket-configuration "LocationConstraint=${REGION}" || {
      echo "[ERROR] No se pudo crear el bucket."
      exit 1
    }
fi

# ===============================================================
# 2. Subir los archivos a sus rutas lógicas
# ===============================================================
echo "[INFO] Subiendo artefactos al bucket..."

aws s3 cp extraer.zip  "s3://${BUCKET_NAME}/lambda/extraer.zip"   --region "${REGION}"
aws s3 cp python.zip   "s3://${BUCKET_NAME}/layers/python.zip"    --region "${REGION}"
aws s3 cp template.yml "s3://${BUCKET_NAME}/plantillas/template.yml" --region "${REGION}"

# ===============================================================
# 3. Verificación final
# ===============================================================
echo "[INFO] Verificando objetos en el bucket..."
aws s3 ls "s3://${BUCKET_NAME}/lambda/"       --region "${REGION}"
aws s3 ls "s3://${BUCKET_NAME}/layers/"       --region "${REGION}"
aws s3 ls "s3://${BUCKET_NAME}/plantillas/"   --region "${REGION}"

echo "[SUCCESS] Todos los artefactos fueron cargados correctamente en s3://${BUCKET_NAME}"
