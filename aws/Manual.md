# Manual de preparación recursos para ingesta de archivos

## Objetivo
Este procedimiento permite levantar los servicios necesarios en AWS y Databricks para el funcionamiento del proyecto

---

## Requisitos previos

- Una cuenta AWS con permisos de:
  - Buckets S3.
  - IAM.
  - CloudFormation
  - Lambda
  - Amazon EventBridge
- Acceso a la consola web de AWS.
- Acceso al repositorio GitHub `https://github.com/milancontreras/Unir-TFM-Vehiculos.git`
- Cuenta en Databricks 
- Tener clonado el repositorio de GitHub  `https://github.com/milancontreras/Unir-TFM-Vehiculos.git` en el Workspace.

**Nota:**
Para el levantamiento del ambiente será necesario el uso de nombres únicos para varias variables. Se recomienda usar los mismos nombres y solo cambiar la parte final con un código único.

Para este ejemplo se usarán los siguientes nombre de variables:
- \<nombre-del-bucket-artifacts\>: `tfm-s3-artefactos-251207`
- \<region\>: `eu-west-2` (No cambiar esta variable)
- \<nombre-del-bucket-de-datos-capa-bronce\>: `tfm-s3-datalake-251207`
- \<nombre-de-la-funcion-lambda\>: `tfm-fun-ingesta-251207`
- \<nombre-del-layer\>: `tfm-layer-ingesta-251207`
- \<nombre-de-la-regla-eventbridge\>: `tfm-automatizacion-diaria-7am-ecuador-251207`
- \<nombre-pila-cloud-formation\>: `tfm-stack-extraccion-251207 `
- \<nombre-host-databrciks\>:  Se lo obtienen abriendo el entorno de Databricks y observando la URL y cortarla al final de .com por ejemplo si la URL es `https://dbc-XXXXXX-XXX.cloud.databricks.com/?o=XXXXXXXXXXXXXX` el host será `https://dbc-XXXXXX-XXX.cloud.databricks.com`.  

- \<token-desarrollador-databricks\>: Se lo obtiene en el entorno de databricks dando clic en la parte superior derecha en el usuario y `Settings -> Developer -> Access tokens -> Generate new token`
- \<nombre-rol-ubicacion-externa-databricks\>: `RolDatabricksExterno-251207`
- \<nombre-ubicacion-externa-databricks>: `tfm_s3_datalake_externo` (No cambiar esta variable)

## Pasos para la ejecución

### 1. Abrir AWS CloudShell
1. Inicia sesión en la consola de AWS.  
2. En la barra superior, haz clic en el ícono **“>_”** (CloudShell).  
3. Espera a que cargue el entorno (normalmente `/home/cloudshell-user/`).

El entorno mostrará uen pantalla algo similar a:
```bash
cloudshell-user@ip-172-31-xx-xx:~$
```

### 2.Cargar archivos desde Github
1.	Ejecutar los comandos.
```bash
git clone --no-checkout https://github.com/milancontreras/Unir-TFM-Vehiculos.git
cd Unir-TFM-Vehiculos
git sparse-checkout init --cone
git sparse-checkout set aws
git checkout
```

2.	Verificamos que se hayan descargado los archivos con el comando.
```bash
ls
```

Se observará:
```bash
aws README.md
```

4. Ingresamos a la carpeta aws y mostramos su contenido
```bash
cd aws
ls
```

Se debría observar:
```bash
artefactos  Manual.md  python.zip  README.md  template.yml  upload_artifacts.sh
```


### 3. Compresión del contenido de la carpeta artefactos
Para poder agreagr el código en una función lambda es necesario que este este en un archivo zip. por lo que compimiremos el contendio de la caprte artefactos en el archivo extraer.zip


Ejecutamos los siguientes comandos:
```bash
cd artefactos
zip -r ../extraer.zip .
cd ..
ls
```

Se puede observar que ahora se tienen el archivo zip:
```bash
extraer.zip
```

### 4. Creación de bucket con los archivos necesarios para la creación de los servicios en AWS.
Para crear todos los servicios de aws como funciones lambda, buckets, roles en IAM, Eventos en EventBridge. Es necesario cargar todos los recursos necesarios y configuraciones inicialmente en un bucket s3. 


Ejecutamos el siguiente comando que creará un bucket s3 inicial con las configuraciones y archivos necesarios para la creación de los demás servicios. Recuerda cambiar el nombre de la variable como se mencionó en el apartado de `Requisitos previos`
```bash
chmod +x upload_artifacts.sh
./upload_artifacts.sh <nombre-del-bucket-artifacts>
```
Se debría observar el siguiente resultado:
```bash
[SUCCESS] Todos los artefactos fueron cargados correctamente en <nombre-del-bucket-artifacts>
```

### 5(Opcional). Revisar la creación del bucket en la interfaz gráfica de AWS.

1. En la interfaz gráfica de AWS nos dirigimos a `Amazon S3 -> <nombre-del-bucket-artifacts>`
2. Vamos a poder observar que dentro del Bucket S3 se encuentran 3 carpetas `lambda\` que contiene el código para la función lambda, `layers\` que contiene las dependencias necesarias para que funcione el código de la función lambda, y `plantillas\` que contiene la plantilla para el uso de CloudFormation y levantar los recursos necesarios.


### 6. Creación de servicios de AWS mediante CloudFormation
1. Ejecutamos el siguiente comando tomando en cuenta el nombre de las variables mencionadas en el apartado de `Requisitos previos`.


```bash
aws cloudformation create-stack \
  --stack-name <nombre-pila-cloud-formation> \
  --template-url https://<nombre-del-bucket-artifacts>.s3.<region>.amazonaws.com/plantillas/template.yml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region <region> \
  --parameters \
    ParameterKey=DataBucketName,ParameterValue=<nombre-del-bucket-de-datos-capa-bronce> \
    ParameterKey=FunctionName,ParameterValue=<nombre-de-la-funcion-lambda> \
    ParameterKey=LayerName,ParameterValue=<nombre-del-layer> \
    ParameterKey=RuleName,ParameterValue= <nombre-de-la-regla-eventbridge>\
    ParameterKey=ArtifactsBucket,ParameterValue=<nombre-del-bucket-artifacts> \
    ParameterKey=LambdaCodeKey,ParameterValue=lambda/extraer.zip \
    ParameterKey=LayerCodeKey,ParameterValue=layers/python.zip \
    ParameterKey=DatabricksHostName,ParameterValue=<nombre-host-databrciks> \
    ParameterKey=DatabricksToken,ParameterValue=<token-desarrollador-databricks> \
    ParameterKey=DatabricksJobId,ParameterValue=0 \
    ParameterKey=DatabricksExternalIdRoleName,ParameterValue=<nombre-rol-ubicacion-externa-databricks> \
    ParameterKey=DatabricksExternalId,ParameterValue=0000000a-0a00-00e0-0a0a-a000aa0aa000
```

2. Verificamos su estado:
```bash
aws cloudformation describe-stacks \
  --stack-name <nombre-pila-cloud-formation> \
  --region eu-west-2 \
  --query "Stacks[0].StackStatus"
```

3.Ejecutamos el comando anterior hasta tener el resultado:
```bash
"CREATE_COMPLETE"
```

### 7. Configuración conexión Databricks y S3
1. Abrimos el entorno Databricks
2. En el menu lateral izquierno nos dirigimos a `Catalog`
3. Donde se muestran todos los catálogos damos clic en el ícono `+` y luego en `create an external location`
4. Seleccionamos la opción `Manual`
5. Rellenamos los campos de la siguientr forma:
  - En External location name* colocamos el valor de la variable `<nombre-ubicacion-externa-databricks>`
  - En URL colocamos `s3://` seguido del valor de la variable `<nombre-del-bucket-de-datos-capa-bronce>`
  - En Storage credential* seleccionamos la opción de `+ Create new storage credential`
  - En IAM role (ARN) colocamos en ARN que encontramos en AWS en el araprtado de `IAM -> Roles -> <nombre-rol-ubicacion-externa-databricks> -> Copiar ARN`

6. Damos Click en Create y se desplegará una ventana emergente en la cual deberemos copiar el `Trust policy` y copiarlo en el ambiente de AWS en `IAM -> Roles -> <nombre-rol-ubicacion-externa-databricks> -> Relaciones de confianza -> Editar la política de confianza`.
7. De vuelta en el ambiente de databricks damos clic en `IAM role configured`.



### 8. Configuración variables en Databricks
1. En el menú lateral izquierdo nos dirigimos a `Workspace` e ingresamos a la carpeta que fue clonada desde github en este lugar, abrimos el archivo ubicado en `Unir-TFM-Vehiculos/Databricks/Configuraciones/variables_configuraciones` 
2. Dentro de este archivo en el apartado de `Variables configuracion volumen externo s3 capa bronce` configuramos la variable `bucket_s3_bronce` con `s3://` seguido del valor de la variable `<nombre-del-bucket-de-datos-capa-bronce>`. 
3. (Opcional) Dentro del mismo archivo se puede revisar la configuración de variables como nombres de jobs a crearse y correos a donde llegaran las notificaciones del job. Si no se modifican estos tomarán los valores por defecto.
4. Abrimos el notebook `Unir-TFM-Vehiculos/Databricks/ConstruirAmbiente` y lo ejecutamos todas sus celdas. Esto creará dos jobs el de Levantar Ambiente que se ejecutará automáticamente y un job para cargar los datos desde bronce y llevarlos a plata y oro.  
Al final del notebook tendremos un mensaje como este: `Nuevo job creado con ID: XXXXXXXXXXXXXXX`. El cual deberá ser guardado ya que será utilizado en un paso más adelante.
5. En el menu izquierdo abriremos `Jobs & Pipelines` y esperaremos a que el Job de `Levantar Ambiente` haya terminado su ejecución de forma exitosa. 


### 9. Configurar variable de entorno de JobId en la funcion Lambda de AWS
1. En AWS nos dirigimos a la función lambda `Lambda -> Funciones -> <nombre-de-la-funcion-lambda> -> Configuración -> Variables de entorno -> Editar`
2. En la variable `JOB_ID` colocaremos el ID que nos dió el notebook de databricks en el anterior punto. Y damos clic en `Guardar`.


### 10. (Opcional) Ejecutar la funcion lambda manualmente
Hasta este punto el proceso ya se ha levantado exitosamente y no es necesario ejecutar manualmente nada. Sin embargo, si se quiere dar una primera ejecución manual, se deben seguir los siguientes pasos.

1. En la consola de AWS   ejecutar el siguiente comando (es normal que tome aprox 2 min):
```bash
aws lambda invoke \
  --function-name <nombre-de-la-funcion-lambda> \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  output.json
  ``` 

#### Resultado
1. Como resultado, dentro del ambiente de AWS se van a cargar los archivos en el bucket S3 de la siguiente forma.
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
2. En el ambiente de databricks el job del Flujo de carga de las capas `Bronce -> Plata -> Oro` se ejecutará y una vez finalizado los datos estarán disponibles en la capa Oro.