# Manual de preparación recursos para ingesta de archivos

## Objetivo
Este procedimiento permite crear un bucket S3 e insertar en él los archivos necesarios para el despliegue del proyecto mediante AWS CloudFormation y Lambda.  
El proceso se realiza desde **AWS CloudShell**, sin requerir configuraciones locales adicionales.

---

## Requisitos previos

- Una cuenta AWS con permisos para:
  - Crear buckets S3.
  - Subir archivos a S3.
- Acceso a la consola web de AWS.
- Archivo comprimido `artefactos.zip` que contiene:
  - `extraer.zip` → código fuente de la función Lambda.
  - `python.zip` → dependencias del layer.
  - `template.yml` → plantilla de CloudFormation.
  - `upload_artifacts.sh` → script de automatización.

---

## Pasos para la ejecución

### 1. Abrir AWS CloudShell
1. Inicia sesión en la consola de AWS.  
2. En la barra superior, haz clic en el ícono **“>_”** (CloudShell).  
3. Espera a que cargue el entorno (normalmente `/home/cloudshell-user/`).

**Referencia visual:**  
El entorno mostrará un prompt similar a:
```bash
cloudshell-user@ip-172-31-xx-xx:~$
```

### 2.Subir el archivo artefactos.zip al entorno
1.	En la barra superior de CloudShell, haz clic en `Acciones > Cargar Archivo`.
2.	Elige el archivo artefactos.zip desde tu computadora.
3.	Espera a que termine la carga (se mostrará en el directorio actual).

Verificar que esté presente con:
```bash
ls
```

Se debría observar:
```bash
artefactos.zip
```

### 3. Extraer el contenido del ZIP

Ejecuta el siguiente comando para descomprimir el archivo:
```bash
unzip artefactos.zip
```

### 4. Verificar los archivos extraídos
Confirma que la carpeta de los archivos extraidosse encuentran en el directorio actual:
```bash
ls
```

Se debría observar:
```bash
artefactos
```

Ingresar en la carpeta
```bash
cd artefactos
ls
```

Se debría observar:
```bash
extraer.zip  python.zip  template.yml  upload_artifacts.sh
```

### 5. Dar permisos de ejecución al script
Antes de ejecutar el script, asegurate de otorgarle permisos de ejecución:
```bash
chmod +x upload_artifacts.sh
```

Verifica los permisos:
```bash
ls -l upload_artifacts.sh
```

Debe mostrarse algo como:
```bash
-rwxr-xr-x  1 cloudshell-user  staff  2048 Nov  4 12:30 upload_artifacts.sh
```

### 6. Ejecutar el script para crear el bucket y subir los archivos

Ejecutá el script pasando como parámetro el nombre del bucket S3 donde se almacenarán los artefactos como recomendación usar el nombre `tfm-s3-artefactos-<codigo-unico>`:
```bash
./upload_artifacts.sh <nombre-del-bucket>
```

Ejemplo:
```bash
./upload_artifacts.sh tfm-s3-artefactos-12345unir
```

El script:
-	Crea el bucket (si no existe).
-   Sube los archivos:
    -   extraer.zip → lambda/extraer.zip
    -   python.zip → layers/python.zip
    -   template.yml → plantillas/template.yml
-   Verifica los objetos cargados en S3.

Salida esperada:
```bash
[INFO] Subiendo artefactos al bucket...
upload: ./extraer.zip to s3://tfm-s3-artefactos-12345unir/lambda/extraer.zip
upload: ./python.zip to s3://tfm-s3-artefactos-12345unir/layers/python.zip
upload: ./template.yml to s3://tfm-s3-artefactos-12345unir5/plantillas/template.yml
[SUCCESS] Todos los artefactos fueron cargados correctamente en s3://tfm-s3-artefactos-12345unir
```

#### Resultado
```bash
s3://<nombre-del-bucket>/
│
├── lambda/
│   └── extraer.zip
│
├── layers/
│   └── python.zip
│
└── plantillas/
    └── template.yml
```

### 7. Crear los recursos necesarios mediante una plantilla
Ejecutar el siguiente comando en `AWS CloudShell`:
**Nota:**  Es recomendable usar los siguientes nombres para que se creen los recursos.
- `<nombre-del-bucket-artifacts> ` usamos el nombre creado en el paso 6 `tfm-s3-artefactos-12345unir` 

- `<region>` -> eu-west-2
- `<nombre-del-bucket-de-datos>` este es el bucket donde se almacenaran los datos extraidos. `tfm-s3-datalake-12345unir`
- <nombre-de-la-funcion-lambda> nombre de la funcion lambda -> `tfm-fun-ingesta-12345unir`
- <nombre-del-layer> nombre del layer que contien las dependecias de la función -> `tfm-layer-ingesta-12345unir`
- <nombre-de-la-regla-eventbridge> nombre de la regla de automatizacion de la funcion -> `tfm-automatizacion-diaria-7am-ecuador-12345unir`
- <nombre-del-bucket-artifacts> nombre del bucket creado en el paso 6 -> `tfm-s3-artefactos-12345unir` 

```bash
aws cloudformation create-stack \
  --stack-name <nombre-pila> \
  --template-url https://<nombre-del-bucket-artifacts>.s3.<region>.amazonaws.com/plantillas/template.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region eu-west-2 \
  --parameters \
    ParameterKey=DataBucketName,ParameterValue=<nombre-del-bucket-de-datos> \
    ParameterKey=FunctionName,ParameterValue=<nombre-de-la-funcion-lambda> \
    ParameterKey=LayerName,ParameterValue=<nombre-del-layer> \
    ParameterKey=RuleName,ParameterValue=<nombre-de-la-regla-eventbridge> \
    ParameterKey=ArtifactsBucket,ParameterValue=<nombre-del-bucket-artifacts> \
    ParameterKey=LambdaCodeKey,ParameterValue=lambda/extraer.zip \
    ParameterKey=LayerCodeKey,ParameterValue=layers/python.zip
```

Ejemplo:
```bash
aws cloudformation create-stack \
  --stack-name tfm-stack-extraccion \
  --template-url https://tfm-s3-artefactos-12345unir.s3.eu-west-2.amazonaws.com/plantillas/template.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region eu-west-2 \
  --parameters \
    ParameterKey=DataBucketName,ParameterValue=tfm-s3-datalake-12345unir \
    ParameterKey=FunctionName,ParameterValue=tfm-fun-ingesta-12345unir \
    ParameterKey=LayerName,ParameterValue=tfm-layer-ingesta-12345unir \
    ParameterKey=RuleName,ParameterValue=tfm-automatizacion-diaria-7am-ecuador-12345unir \
    ParameterKey=ArtifactsBucket,ParameterValue=tfm-s3-artefactos-12345unir \
    ParameterKey=LambdaCodeKey,ParameterValue=lambda/extraer.zip \
    ParameterKey=LayerCodeKey,ParameterValue=layers/python.zip

```

Verificamos su estado:
```bash
aws cloudformation describe-stacks \
  --stack-name <nombre-pila> \
  --region eu-west-2 \
  --query "Stacks[0].StackStatus"
```

ejemplo;
```bash
aws cloudformation describe-stacks \
  --stack-name tfm-stack-extraccion \
  --region eu-west-2 \
  --query "Stacks[0].StackStatus"
```

Ejecutamos el comando anterior hasta tener el resultado:
```bash
"CREATE_COMPLETE"
```


### 8. Ejecutar la funcion lambda manualmente

Ejecutar en la consola (es normal que tome aporx 2 min):
```bash
aws lambda invoke \
  --function-name <nombre-de-la-funcion-lambda> \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  output.json
  ``` 

Ejemplo:
```bash
aws lambda invoke \
  --function-name tfm-fun-ingesta-12345unir \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  output.json
``` 

#### Resultado
```bash
s3://<nombre-del-bucket-de-datos>/
│
├── metadata/
│   └── vehiculos/
|       └── manifest-20251104_111707.jsonl
|       └── state.json
│
└── raw/
    └── vehiculos/
        ├── year=2017/
        |  └──SRI_Vehiculos_Nuevos_2017__ingest_ts=20251104_111707.csv
        ├── year=2018/
        |  └──SRI_Vehiculos_Nuevos_2018__ingest_ts=20251104_111707.csv
        ├── year=2019/
        |  └──SRI_Vehiculos_Nuevos_2019__ingest_ts=20251104_111707.csv
        ├── year=2020/
        |  └──SRI_Vehiculos_Nuevos_2020__ingest_ts=20251104_111707.csv
        ...   

```
