# Databricks — Estructura del Proyecto TFM
Este directorio contiene la organización completa del proyecto en Databricks para el pipeline de ingesta, normalización, homologación y carga del dataset de vehículos nuevos (SRI), cubriendo las capas Bronce y Plata, así como los elementos de configuración y catálogos necesarios para su procesamiento.

A continuación se describe el propósito de cada carpeta y archivo.
_______________________

## 1. LevantarAmbiente
Contiene los notebooks necesarios para inicializar el entorno de trabajo en Databricks. Aquí se crean las tablas del metastore y se cargan las configuraciones base para el pipeline.

Contenido:
- **01_creacion_tablas:**  
Notebook que ejecuta los notebooks de creación de todas las tablas del proyecto (configuración, catálogos, estado, homologación, entre otras).  
- **02_insertar_valores_casteo_columnas:**  
Ejecuta los notebooks que insertan la lógica declarativa de casteo y normalización de tipos para cada columna de las fuentes del SRI.
- **03_insertar_valores_mapeo_cabeceras:**  
Ejecuta los notebooks que insertan los datos para definir el mapeo estándar de encabezados variables provenientes de los múltiples CSVs del SRI.
- **04_insertar_valores_catalogos:**  
Ejecuta los notebooks que cargan los catálogos base utilizados en la capa Silver (clase, marca, país, combustible, comprador, etc.).
-  **05_insertar_valores_hmg:**  
Ejecuta los notebooks que cargan las tablas de homologación (HMG), necesarias para unificar valores heterogéneos provenientes de las distintas fuentes.

## 2. Porcesos 
Contiene los notebooks principales de ejecución del pipeline.

Contenido:
- **carga_datos_vehiculos:**  
Pipeline de carga que incluye lectura desde la capa Bronce, validación, normalización, homologación y escritura final en Silver.

## 3. Scripts

Incluye notebooks individuales responsables de poblar catálogos y ejecutar homologaciones tanto en la sub-capa de catálogos (ctg) como en la sub-capa de homologación (hmg) de Silver.

Tipos de scripts:

- Configuración  
  - tfm_config.casteo_columnas  
  - tfm_config.mapeo_cabeceras

- Catálogos Silver (tfm_silver_ctg.*)  
Scripts para poblar:  
    - Clase
    - Marca  
    - País  
    - Subclase  
    - Tipo de combustible  
    - Tipo de comprador  
    - Tipo de servicio  
    - Tipo de transacción  
    - Tipo de vehículo  

- Homologación Silver (tfm_silver_hmg.*)  
Scripts que normalizan valores heterogéneos:
    - Clase  
    - Marca  
    - País  
    - Subclase  
    - Tipo de combustible  
    - Tipo de comprador  
    - Tipo de servicio  
    - Tipo de transacción  
    - Tipo de vehículo  

## 4. Tablas

Esta carpeta agrupa notebooks que representan las estructuras de las tablas utilizadas en las capas de Bronce, Configuración y Silver.

Contenido representativo:

- Bronce  
  - **tfm_bronze.tabla_estado:**   
  Tabla de control del estado de los archivos ingeridos.

- Configuración  
  - tfm_config.mapeo_cabeceras  
  - tfm_config.normalizacion_columnas

- Silver
  Tablas de:  
  - Datos de vehículos (tfm_silver.tabla_datos_vehiculos)  
    - Estado (tfm_silver.tabla_estado)  
    - Catálogos (tfm_silver_ctg.*)  
    - Homologación (tfm_silver_hmg.homologacion)  
