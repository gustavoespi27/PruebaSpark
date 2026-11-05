# Guía de Estudio — Prueba Big Data

## SparkSession, Spark SQL, programas PySpark y pipeline batch

## Propósito

Reunir en un solo documento, listo para copiar y pegar, todo el procedimiento necesario para la prueba: desde la conexión al laboratorio hasta la ejecución y validación del pipeline batch de la Semana 11.

## Contenidos evaluados

| # | Contenido | Guía de origen | Parte de este documento |
|---:|---|---|---|
| 1 | Resolver un problema con Spark SQL | Semana 10 — Nivel 3 — Ejercicio 12 | **Parte 2** |
| 2 | Programa PySpark ejecutable | Semana 10 — Nivel 4 — Ejercicios 16 y 17 | **Parte 3** |
| 3 | Pipeline batch con múltiples fuentes | Semana 11 — Nivel 3 — Ejercicios 11 a 14 | **Parte 4** |
| 4 | SparkSession | Base de todos los programas (Semanas 10 y 11) | **Parte 1** |

## Organización del documento

| Parte | Contenido |
|---|---|
| **Paso 0** | Preparar el entorno: conexión, verificación de HDFS y acceso a Spark |
| **Parte 1** | SparkSession: qué es, cómo se crea y cómo se cierra |
| **Parte 2** | Ejercicio 12: el mismo problema con DataFrame API y Spark SQL |
| **Parte 3** | Ejercicios 16 y 17: crear, ejecutar y documentar un programa `.py` |
| **Parte 4** | Ejercicios 11 a 14 (Semana 11): pipeline batch completo |
| **Anexos** | Resumen de comandos, errores frecuentes y conceptos clave |

---

## Antes de comenzar

Se mantienen las responsabilidades de los componentes utilizadas durante el curso:

```text
HDFS  → namenode
Spark → spark-master
```

Por tanto:

- las verificaciones de HDFS se realizan desde `namenode`;
- los programas PySpark se escriben y ejecutan desde `spark-master`;
- Spark lee y escribe en HDFS mediante la URI `hdfs://namenode:8020/...`.

### Convenciones de este documento

| 📍 Lugar | Se reconoce por |
|---|---|
| PowerShell (PC) | `PS C:\Users\...>` |
| Instancia EC2 | `ubuntu@ip-...:~$` |
| Contenedor `spark-master` | `root@...:/...#` |
| PySpark interactivo | `>>>` |

> **Regla para copiar y pegar:** copie solo el contenido de los bloques de código, pegue **un bloque a la vez** y verifique que la línea esté vacía antes de pegar (`Ctrl + U`).

---

# Paso 0 — Preparar el entorno

## 1. Encender el laboratorio

1. AWS Academy → **Learner Lab** → **Start Lab** → esperar el indicador 🟢.
2. **AWS** → **EC2** → **Instancias** → **Hadoop.large**.
3. Copiar la **Dirección IPv4 pública**.
4. Esperar **2/2 comprobaciones superadas**.

> La IP pública cambia cada vez que se inicia el laboratorio.

## 2. Conectarse desde PowerShell

📍 **PowerShell** — reemplazar `IP_PUBLICA`:

```powershell
ssh -i $HOME\.ssh\aws_hadoop -L 8080:localhost:8080 -L 50070:localhost:50070 ubuntu@IP_PUBLICA
```

Resultado esperado:

```text
ubuntu@ip-172-31-20-82:~$
```

Mientras esta ventana permanezca abierta:

```text
Spark Master → http://localhost:8080
HDFS         → http://localhost:50070
```

## 3. Verificar las fuentes en HDFS

📍 **Instancia EC2**

```bash
sudo docker exec namenode hdfs dfs -ls -R /curso | grep -E "ventas_hive.csv|ciudades.csv"
```

Resultado esperado:

```text
... 6050787 ... /curso/hive/datos_ventas/ventas_hive.csv
...     203 ... /curso/spark/guia3/fuentes/ciudades.csv
```

> Si falta `ciudades.csv`, ejecute el **Anexo A** antes de continuar.

## 4. Ingresar a `spark-master`

📍 **Instancia EC2**

```bash
sudo docker exec -it spark-master bash
```

## 5. Configurar Python 3 y preparar la carpeta de trabajo

📍 **spark-master**

```bash
export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && export PYTHONIOENCODING=utf-8 && mkdir -p /tmp/curso_spark/nivel3 && cd /tmp/curso_spark
```

> Las variables `PYSPARK_PYTHON` se pierden al salir del contenedor. Repita este paso cada vez que ingrese.
>
> `PYTHONIOENCODING=utf-8` evita errores al imprimir textos con tildes (por ejemplo, `VALIDACIÓN`).

## Estado esperado del entorno

```text
PowerShell
    │ SSH
    ▼
AWS EC2
    │ docker exec
    ▼
spark-master  ──── hdfs://namenode:8020 ────►  HDFS
    │                                           ├── ventas_hive.csv
    ▼                                           └── ciudades.csv
/tmp/curso_spark
```

---

# Parte 1 — SparkSession

## 1. ¿Qué es una SparkSession?

La `SparkSession` es el **punto de entrada** a Spark. Todo programa PySpark la necesita para:

- leer datos (`spark.read`);
- crear DataFrames (`spark.createDataFrame`);
- ejecutar Spark SQL (`spark.sql`);
- registrarse en el clúster con un nombre (`appName`).

```text
Programa PySpark
      │
      ▼
SparkSession  ──►  Spark Master  ──►  Worker / Executor
      │
      ├── spark.read
      ├── spark.createDataFrame
      └── spark.sql
```

## 2. Dos formas de obtenerla

| Modalidad | ¿Quién crea la SparkSession? |
|---|---|
| `./pyspark` (interactivo) | PySpark la crea automáticamente con el nombre `spark` |
| `spark-submit programa.py` | **El programa debe crearla** con `SparkSession.builder` |

Por eso todos los programas `.py` comienzan igual:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NombreDeLaAplicacion") \
    .getOrCreate()
```

y terminan con:

```python
spark.stop()
```

## 3. Significado de cada elemento

| Elemento | Función |
|---|---|
| `SparkSession.builder` | Inicia la construcción de la sesión |
| `.appName("...")` | Nombre con el que la aplicación aparece en Spark Master (`localhost:8080`) |
| `.getOrCreate()` | Crea la sesión o **reutiliza** una existente |
| `spark.stop()` | Finaliza la aplicación y **libera** los recursos (cores y memoria) del clúster |

## Ejercicio S1 — Crear, observar y cerrar una SparkSession

### 1. Cree el programa

📍 **spark-master**

```bash
cd /tmp/curso_spark && cat > sesion_spark.py <<'EOF'
from pyspark.sql import SparkSession

# DECISIÓN:
# Se crea la SparkSession porque un programa ejecutado con spark-submit
# no dispone automáticamente del objeto spark, como ocurre en ./pyspark.
spark = SparkSession.builder \
    .appName("Prueba_SparkSession") \
    .getOrCreate()

print("==================================================")
print("Nombre de la aplicacion:", spark.sparkContext.appName)
print("Master utilizado:", spark.sparkContext.master)
print("Version de Spark:", spark.version)
print("ID de la aplicacion:", spark.sparkContext.applicationId)

# getOrCreate() reutiliza la sesion existente en lugar de crear otra
otra = SparkSession.builder.getOrCreate()
print("Es la misma sesion:", otra is spark)
print("==================================================")

# DECISIÓN:
# Se finaliza la sesion para liberar los recursos del cluster.
spark.stop()
EOF
```

### 2. Ejecute

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 sesion_spark.py 2>/dev/null
```

> `2>/dev/null` oculta los mensajes `INFO` y deja visible solo la salida del programa.

### Resultado esperado

```text
==================================================
Nombre de la aplicacion: Prueba_SparkSession
Master utilizado: spark://spark-master:7077
Version de Spark: 2.x.x
ID de la aplicacion: app-2026...
Es la misma sesion: True
==================================================
```

### 3. Observe Spark Master

Abra `http://localhost:8080` y localice **Prueba_SparkSession** en **Completed Applications**. 📸

### Analice

**a.** ¿Por qué un programa `.py` debe crear la SparkSession y en `./pyspark` no es necesario?

> En `./pyspark` la sesión se crea automáticamente al iniciar la consola. Un programa ejecutado con `spark-submit` es independiente y debe crearla explícitamente con `SparkSession.builder`.

**b.** ¿Qué función cumple `appName`?

> Identifica la aplicación en el clúster. Es el nombre que aparece en Spark Master (`localhost:8080`).

**c.** ¿Qué demuestra `Es la misma sesion: True`?

> Que `getOrCreate()` reutiliza la sesión existente en lugar de crear una segunda. Dentro de una aplicación existe una única SparkSession activa.

**d.** ¿Por qué se ejecuta `spark.stop()` al final?

> Para finalizar la aplicación y liberar los cores y la memoria del clúster, de modo que otras aplicaciones puedan utilizarlos.

**e.** ¿Qué significa `spark://spark-master:7077`?

> Que la aplicación se ejecuta en el clúster Spark Standalone y no en modo local.

### Concepto clave

```text
SparkSession.builder → appName → getOrCreate()
        │
        ▼
   aplicación Spark activa  (visible en :8080)
        │
        ▼
   lectura / procesamiento / escritura
        │
        ▼
   spark.stop()  → recursos liberados
```

---

# Parte 2 — Semana 10 · Nivel 3 · Ejercicio 12

# Ejercicio 12 — Resolver el mismo problema mediante Spark SQL

## Objetivo

Comprobar que un mismo problema analítico puede expresarse mediante **DataFrame API** o mediante **Spark SQL**, y que ambos producen el mismo resultado.

## Problema

> **¿Cuál es el monto total vendido por ciudad considerando solamente las ventas de la categoría Tecnología?**

## 1. Planifique

```text
100.000 ventas (HDFS)
      │
      ▼
WHERE categoria = 'Tecnologia'      ←→   filter
      │
      ▼
GROUP BY ciudad                     ←→   groupBy
      │
      ▼
SUM(monto)                          ←→   agg(sum)
      │
      ▼
ORDER BY total_ventas DESC          ←→   orderBy(desc)
      │
      ▼
resultado
```

## 2. Modalidad interactiva

### 2.1 Inicie PySpark

📍 **spark-master**

```bash
cd /spark/bin && ./pyspark --master spark://spark-master:7077
```

### 2.2 Prepare el DataFrame y la vista temporal

📍 **PySpark (`>>>`)** — pegue el bloque completo:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, sum, avg, count, desc
schema_ventas = StructType([StructField("id_venta", IntegerType(), True), StructField("fecha", StringType(), True), StructField("cliente", StringType(), True), StructField("ciudad", StringType(), True), StructField("categoria", StringType(), True), StructField("monto", DoubleType(), True)])
ventas_schema_df = spark.read.option("header", "false").schema(schema_ventas).csv("hdfs://namenode:8020/curso/hive/datos_ventas/ventas_hive.csv")
ventas_schema_df.createOrReplaceTempView("ventas")
ventas_schema_df.count()
```

Resultado esperado: `100000`

### 2.3 Solución mediante DataFrame API (Ejercicio 11)

```python
resultado_api = ventas_schema_df.filter(col("categoria") == "Tecnologia").groupBy("ciudad").agg(sum("monto").alias("total_ventas")).orderBy(desc("total_ventas"))
```

```python
resultado_api.show()
```

### 2.4 Solución mediante Spark SQL (Ejercicio 12)

```python
resultado_sql = spark.sql("""
SELECT ciudad, SUM(monto) AS total_ventas
FROM ventas
WHERE categoria = 'Tecnologia'
GROUP BY ciudad
ORDER BY total_ventas DESC
""")
```

```python
resultado_sql.show()
```

### 2.5 Compruebe que ambos planes de ejecución son equivalentes

```python
resultado_api.explain()
```

```python
resultado_sql.explain()
```

Ambos planes contienen las mismas etapas: `Filter`, `HashAggregate`, `Exchange` y `Sort`.

### 2.6 Salga de PySpark

```python
exit()
```

## 3. Modalidad programa (`.py`)

### 3.1 Cree el programa

📍 **spark-master**

```bash
cd /tmp/curso_spark && cat > ejercicio12.py <<'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, sum, desc

spark = SparkSession.builder \
    .appName("Ejercicio12_SparkSQL") \
    .getOrCreate()

# DECISIÓN:
# Se define un schema explícito porque la estructura del archivo es
# conocida y el CSV no posee encabezado.
schema_ventas = StructType([
    StructField("id_venta",  IntegerType(), True),
    StructField("fecha",     StringType(),  True),
    StructField("cliente",   StringType(),  True),
    StructField("ciudad",    StringType(),  True),
    StructField("categoria", StringType(),  True),
    StructField("monto",     DoubleType(),  True)
])

ventas_schema_df = spark.read \
    .option("header", "false") \
    .schema(schema_ventas) \
    .csv("hdfs://namenode:8020/curso/hive/datos_ventas/ventas_hive.csv")

print("\n=== REGISTROS LEIDOS DESDE HDFS ===")
print(ventas_schema_df.count())

# ---------------- DataFrame API (Ejercicio 11) ----------------
resultado_api = ventas_schema_df \
    .filter(col("categoria") == "Tecnologia") \
    .groupBy("ciudad") \
    .agg(sum("monto").alias("total_ventas")) \
    .orderBy(desc("total_ventas"))

print("\n=== RESULTADO DATAFRAME API ===")
resultado_api.show()

# ---------------- Spark SQL (Ejercicio 12) ----------------
# DECISIÓN:
# Se registra una vista temporal para poder consultar el DataFrame
# mediante SQL. No crea una tabla Hive ni copia datos en HDFS.
ventas_schema_df.createOrReplaceTempView("ventas")

resultado_sql = spark.sql("""
    SELECT ciudad, SUM(monto) AS total_ventas
    FROM ventas
    WHERE categoria = 'Tecnologia'
    GROUP BY ciudad
    ORDER BY total_ventas DESC
""")

print("\n=== RESULTADO SPARK SQL ===")
resultado_sql.show()

# ---------------- Comparación de planes ----------------
print("\n=== PLAN DATAFRAME API ===")
resultado_api.explain()

print("\n=== PLAN SPARK SQL ===")
resultado_sql.explain()

# INTERPRETACIÓN:
# Ambas soluciones entregan las mismas 8 ciudades con los mismos
# totales. Santiago lidera con aprox. 1.930 millones y Temuco cierra
# con aprox. 1.786 millones. Los planes de ejecución son equivalentes:
# DataFrame API y Spark SQL son dos formas de expresar el mismo
# procesamiento sobre el mismo motor Spark.

spark.stop()
EOF
```

### 3.2 Ejecute

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 ejercicio12.py 2>/dev/null
```

## 4. Resultado esperado (ambas soluciones)

| ciudad | total_ventas |
|---|---:|
| Santiago | 1.930.379.735,93 |
| Valparaiso | 1.920.458.809,34 |
| Rancagua | 1.902.867.485,43 |
| Concepcion | 1.894.988.912,45 |
| Antofagasta | 1.856.645.942,61 |
| La_Serena | 1.827.328.082,75 |
| Puerto_Montt | 1.825.371.269,60 |
| Temuco | 1.786.179.886,80 |

> Spark muestra los valores grandes en notación científica: `1.93037973593E9` = 1.930.379.735,93.

## 5. Complete

| DataFrame API | Spark SQL |
|---|---|
| `filter` | `WHERE` |
| `groupBy` | `GROUP BY` |
| `sum` | `SUM()` |
| `orderBy` | `ORDER BY` |

## 6. Reflexione

**¿Spark SQL constituye un motor completamente diferente de DataFrame API?**

> No. Ambas alternativas se traducen al mismo plan de ejecución, optimizado por Spark (Catalyst), y se ejecutan con los mismos Jobs, Stages y Tasks. Lo que permanece común es el **DataFrame**, el **motor Spark** y los **datos almacenados en HDFS**. Solo cambia la forma de expresar la consulta.

**¿Spark SQL utiliza Hive para ejecutar la consulta?**

> No. En nuestra infraestructura Spark no está conectado al Hive Metastore. La vista temporal `ventas` existe solo en la sesión Spark y los datos se leen directamente desde HDFS.

## Concepto clave

```text
                 MISMO PROBLEMA
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
       DataFrame API         Spark SQL
   filter + groupBy + agg   WHERE + GROUP BY + SUM
             │                   │
             └─────────┬─────────┘
                       ▼
              mismo plan de ejecución
                       ▼
                 mismo resultado
```

---

# Parte 3 — Semana 10 · Nivel 4 — Programa PySpark ejecutable

## Dos formas de trabajar con PySpark

```text
MODALIDAD INTERACTIVA                 MODALIDAD SCRIPT

./pyspark                             archivo .py
    │                                      │
    ▼                                      ▼
>>> instrucción                      escribir programa
>>> resultado                              │
    │                                      ▼
    ▼                                 spark-submit
  exit()                                   │
    │                                      ▼
    ▼                                 resultado
se pierde el código                   el código se conserva
```

> `spark-submit` ejecuta el programa **completo desde el comienzo** cada vez.

---

# Ejercicio 16 — Crear, guardar y ejecutar un programa PySpark

## Objetivo

Crear un archivo `.py`, guardar instrucciones PySpark y ejecutarlas mediante `spark-submit`.

## Paso 1. Ubíquese en la carpeta de trabajo

📍 **spark-master**

```bash
cd /tmp/curso_spark && pwd
```

Resultado esperado: `/tmp/curso_spark`

## Paso 2. Construya el programa por bloques

### Bloque 1 — Iniciar Spark y crear los datos

```bash
cat > ejercicio16.py <<'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("Ejercicio16_Guia2") \
    .getOrCreate()

datos = [
    ("Ana", 150000.0),
    ("Luis", 85000.0),
    ("Carla", 210000.0),
    ("Pedro", 95000.0),
    ("Maria", 180000.0)
]
EOF
```

Ejecute para comprobar que no existen errores:

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 ejercicio16.py 2>/dev/null && echo "BLOQUE 1 OK"
```

Resultado esperado: `BLOQUE 1 OK`

### Bloque 2 — Crear y visualizar el DataFrame

`>>` agrega el bloque al final del archivo sin borrar lo anterior:

```bash
cat >> ejercicio16.py <<'EOF'

df = spark.createDataFrame(
    datos,
    ["cliente", "monto"]
)

df.show()
EOF
```

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 ejercicio16.py 2>/dev/null
```

Resultado esperado: tabla con los 5 clientes.

### Bloque 3 — Incorporar el análisis

```bash
cat >> ejercicio16.py <<'EOF'

df.agg(
    avg("monto").alias("monto_promedio")
).show()

spark.stop()
EOF
```

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 ejercicio16.py 2>/dev/null
```

Resultado esperado:

```text
+-------+--------+
|cliente|   monto|
+-------+--------+
|    Ana|150000.0|
|   Luis| 85000.0|
|  Carla|210000.0|
|  Pedro| 95000.0|
|  Maria|180000.0|
+-------+--------+

+--------------+
|monto_promedio|
+--------------+
|      144000.0|
+--------------+
```

> Aunque solo se agregó el Bloque 3, `spark-submit` volvió a ejecutar los bloques 1, 2 y 3 en orden.

## Paso 3. Compruebe que el archivo fue guardado

```bash
ls -l ejercicio16.py
```

```bash
cat ejercicio16.py
```

## Paso 4. Modifique y vuelva a ejecutar

Agregue a Diego como sexto registro:

```bash
sed -i 's/("Maria", 180000.0)/("Maria", 180000.0),\n    ("Diego", 120000.0)/' ejercicio16.py && grep -n "Diego" ejercicio16.py
```

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 ejercicio16.py 2>/dev/null
```

Resultado esperado: 6 clientes y `monto_promedio = 140000.0`.

## Interprete

**1.** ¿Qué diferencia práctica existe entre escribir instrucciones en `pyspark` y guardarlas en un `.py`?

> En `pyspark` las instrucciones se pierden al salir con `exit()`. En un `.py` quedan guardadas y pueden volver a ejecutarse, modificarse y entregarse.

**2.** ¿Qué función cumple `SparkSession.builder...getOrCreate()`?

> Crea (o reutiliza) la SparkSession, punto de entrada a Spark. En `pyspark` ya existe como `spark`; en un programa debe crearse.

**3.** ¿Qué función cumple `spark-submit`?

> Envía el programa al clúster (`spark://spark-master:7077`) y lo ejecuta completo como una aplicación Spark.

**4.** ¿Qué ventaja ofrece conservar el análisis como archivo ejecutable?

> Es reproducible: produce el mismo resultado cada vez, puede automatizarse, versionarse y entregarse.

**5.** ¿Por qué el programa finaliza con `spark.stop()`?

> Para cerrar la aplicación y liberar los recursos del clúster.

## Concepto clave

```text
ejercicio16.py → spark-submit → Spark Application → Spark Master → Worker / Executor → resultado
```

---

# Ejercicio 17 — Documentar decisiones e interpretaciones dentro del código

## Objetivo

Utilizar comentarios para explicar decisiones técnicas e interpretar resultados dentro del mismo programa.

> En Python, una línea que comienza con `#` es un comentario y no se ejecuta.

## Paso 1. Cree una copia del programa anterior

```bash
cp ejercicio16.py ejercicio17.py && ls -l ejercicio1*.py
```

## Paso 2. Escriba el programa documentado

```bash
cat > ejercicio17.py <<'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("Ejercicio17_Guia2") \
    .getOrCreate()

datos = [
    ("Ana", 150000.0),
    ("Luis", 85000.0),
    ("Carla", 210000.0),
    ("Pedro", 95000.0),
    ("Maria", 180000.0),
    ("Diego", 120000.0)
]

# DECISIÓN:
# Se representa la información mediante un DataFrame porque
# necesitamos trabajar con columnas identificadas por nombre
# y aplicar funciones de agregación sobre monto.
df = spark.createDataFrame(
    datos,
    ["cliente", "monto"]
)

df.show()

# DECISIÓN:
# Se utiliza avg() dentro de agg() porque la pregunta requiere
# una medida resumen de todo el conjunto y no un valor por cliente.
df.agg(
    avg("monto").alias("monto_promedio")
).show()

# INTERPRETACIÓN:
# El monto promedio de los 6 clientes es 140.000.
# Ana (150.000), Carla (210.000) y Maria (180.000) se encuentran
# sobre el promedio; Luis (85.000), Pedro (95.000) y Diego (120.000)
# se encuentran bajo el promedio.

spark.stop()

# PREGUNTA:
# ¿Qué ventaja tendría definir explícitamente un schema
# si este programa debiera procesar un archivo cuya estructura
# conocemos de antemano?
#
# RESPUESTA:
# Spark no necesitaría leer los datos para inferir los tipos, lo que
# reduce el trabajo sobre archivos grandes. Además, los tipos quedan
# garantizados (por ejemplo, monto siempre como DoubleType) y se evita
# que un valor inesperado cambie el tipo de una columna sin aviso.
EOF
```

## Paso 3. Compruebe que los comentarios no impiden la ejecución

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 ejercicio17.py 2>/dev/null
```

Resultado esperado: `monto_promedio = 140000.0`

```bash
cat ejercicio17.py
```

## Reflexione

```text
Programa PySpark
      │
      ├── código ejecutable
      ├── decisiones técnicas      (# DECISIÓN)
      ├── interpretación           (# INTERPRETACIÓN)
      └── respuestas conceptuales  (# PREGUNTA / # RESPUESTA)
```

> Los comentarios deben explicar **por qué** se hace algo, no repetir **qué** hace la línea. `# Crear un DataFrame` no aporta; `# Se usa un DataFrame porque necesitamos columnas con nombre para agregar` sí aporta.

---

# Parte 4 — Semana 11 · Nivel 3 — Pipeline batch

## ¿Qué es un pipeline de datos?

Una secuencia organizada de etapas mediante las cuales los datos son obtenidos, validados, procesados y convertidos en un producto persistente.

```text
FUENTES → LECTURA → VALIDACIÓN → INTEGRACIÓN → TRANSFORMACIÓN → AGREGACIÓN → PERSISTENCIA → VALIDACIÓN FINAL
```

## Arquitectura que construiremos

```text
                        HDFS
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       ventas_hive.csv         ciudades.csv
              │                     │
              ▼                     ▼
          df_ventas            df_ciudades
              │                     │
              └──────────┬──────────┘
                         ▼
                     validación
                         ▼
                     LEFT JOIN
                         ▼
                    nivel_venta
                         ▼
               groupBy(region, nivel)
                         ▼
                      Parquet
                         ▼
                        HDFS
                         ▼
                 relectura + validación
```

| | Ruta |
|---|---|
| Entrada 1 | `hdfs://namenode:8020/curso/hive/datos_ventas/ventas_hive.csv` |
| Entrada 2 | `hdfs://namenode:8020/curso/spark/guia3/fuentes/ciudades.csv` |
| Salida | `hdfs://namenode:8020/curso/spark/guia3/pipeline/ventas_por_region_nivel` |

> El mismo archivo `pipeline_ventas.py` crece en cada ejercicio. Cada bloque `cat > ... EOF` de esta parte **reemplaza** el archivo completo con la versión del ejercicio correspondiente.

---

# Ejercicio 11 — Construir la entrada del pipeline con múltiples fuentes desde HDFS

## 1. Verifique las fuentes

📍 **Instancia EC2** (en otra ventana de PowerShell, o después de `exit` en `spark-master`):

```bash
sudo docker exec namenode hdfs dfs -ls /curso/hive/datos_ventas/ /curso/spark/guia3/fuentes/
```

## 2. Cree el programa del pipeline (versión Ejercicio 11)

📍 **spark-master**

```bash
mkdir -p /tmp/curso_spark/nivel3 && cd /tmp/curso_spark/nivel3 && cat > pipeline_ventas.py <<'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

# ============================================================
# INICIO DE LA APLICACIÓN
# ============================================================
spark = SparkSession.builder \
    .appName("PipelineVentasGuia3") \
    .getOrCreate()

# ============================================================
# FUENTES (las entradas permanecen en HDFS)
# ============================================================
ruta_ventas = (
    "hdfs://namenode:8020/"
    "curso/hive/datos_ventas/ventas_hive.csv"
)

ruta_ciudades = (
    "hdfs://namenode:8020/"
    "curso/spark/guia3/fuentes/ciudades.csv"
)

# ============================================================
# ETAPA 1: CARGA DE FUENTES
# ============================================================
print("\n=== ETAPA 1: CARGA DE FUENTES ===")

# DECISIÓN:
# Schema explícito para ventas: el CSV no tiene encabezado y su
# estructura es conocida.
schema_ventas = StructType([
    StructField("id_venta", IntegerType(), True),
    StructField("fecha", StringType(), True),
    StructField("cliente", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("monto", DoubleType(), True)
])

df_ventas = spark.read \
    .schema(schema_ventas) \
    .option("header", "false") \
    .csv(ruta_ventas)

# ciudades.csv sí posee encabezado
df_ciudades = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ruta_ciudades)

# ============================================================
# ETAPA 2: VALIDACIÓN DE ENTRADAS
# ============================================================
print("\n=== ETAPA 2: VALIDACIÓN DE ENTRADAS ===")

print("\n=== ESQUEMA: VENTAS ===")
df_ventas.printSchema()

print("\n=== ESQUEMA: CIUDADES ===")
df_ciudades.printSchema()

print("\n=== MUESTRA: VENTAS ===")
df_ventas.show(5, False)

print("\n=== MUESTRA: CIUDADES ===")
df_ciudades.show(10, False)

cantidad_ventas = df_ventas.count()
cantidad_ciudades = df_ciudades.count()

print("Cantidad de ventas:", cantidad_ventas)
print("Cantidad de ciudades:", cantidad_ciudades)

print("\n=== CIUDADES PRESENTES EN VENTAS ===")
df_ventas.select("ciudad").distinct().orderBy("ciudad").show(50, False)

print("\n=== CIUDADES PRESENTES EN REFERENCIA ===")
df_ciudades.select("ciudad").distinct().orderBy("ciudad").show(50, False)

# DECISIÓN:
# El pipeline se detiene si una fuente obligatoria está vacía.
if cantidad_ventas == 0:
    raise ValueError(
        "El pipeline no puede continuar: "
        "el dataset de ventas está vacío."
    )

if cantidad_ciudades == 0:
    raise ValueError(
        "El pipeline no puede continuar: "
        "el dataset de ciudades está vacío."
    )

print("\nEntradas del pipeline cargadas y validadas correctamente.")

spark.stop()
EOF
```

## 3. Ejecute

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 pipeline_ventas.py 2>/dev/null
```

## 4. Resultado esperado

```text
Cantidad de ventas: 100000
Cantidad de ciudades: 7
```

Ciudades en **ventas** (8): Antofagasta, Concepcion, **La_Serena**, **Puerto_Montt**, **Rancagua**, Santiago, Temuco, Valparaiso

Ciudades en **referencia** (7): Antofagasta, Concepcion, **La Serena**, **Puerto Montt**, Santiago, Temuco, Valparaiso

> Las diferencias son **intencionales**: `La_Serena` ≠ `La Serena`, `Puerto_Montt` ≠ `Puerto Montt` y `Rancagua` no existe en la referencia. Esas ventas no encontrarán región en el `join`.

## 5. Analice

**a.** ¿Por qué el programa comienza a comportarse como un pipeline?

> Porque organiza etapas encadenadas y reproducibles (cargar, validar y luego decidir si continuar), en lugar de instrucciones aisladas.

**b.** ¿Por qué las fuentes se almacenan en HDFS y no dentro de `spark-master`?

> HDFS es el almacenamiento persistente y compartido. El programa no depende de archivos locales del contenedor, y cualquier motor (Spark o Hive) puede leer las mismas fuentes.

**c.** ¿Qué función cumple el esquema explícito?

> Define nombres y tipos sin inferirlos. El CSV no tiene encabezado y así se garantiza que `monto` sea numérico.

**d.** ¿Por qué validar antes del `join`?

> Para detectar fuentes vacías, columnas faltantes o tipos incorrectos antes de procesar, y evitar resultados erróneos difíciles de rastrear.

**e.** ¿Qué debe hacer el pipeline si una fuente obligatoria tiene cero registros?

> Detenerse con un error explícito (`raise ValueError`), en lugar de continuar y generar un producto vacío o incorrecto.

**f.** ¿Por qué examinar los valores de `ciudad` antes de integrar?

> Porque que ambas fuentes tengan una columna `ciudad` no garantiza que sus valores coincidan. Aquí se detecta que `La_Serena`, `Puerto_Montt` y `Rancagua` no tienen correspondencia.

---

# Ejercicio 12 — Integrar, transformar y agregar datos en un único pipeline

## 1. Amplíe el programa (versión Ejercicio 12)

📍 **spark-master**

```bash
cd /tmp/curso_spark/nivel3 && cat > pipeline_ventas.py <<'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    when,
    count,
    sum,
    avg,
    round
)

spark = SparkSession.builder \
    .appName("PipelineVentasGuia3") \
    .getOrCreate()

ruta_ventas = "hdfs://namenode:8020/curso/hive/datos_ventas/ventas_hive.csv"
ruta_ciudades = "hdfs://namenode:8020/curso/spark/guia3/fuentes/ciudades.csv"

# ============================================================
# ETAPA 1: CARGA DE FUENTES
# ============================================================
print("\n=== ETAPA 1: CARGA DE FUENTES ===")

schema_ventas = StructType([
    StructField("id_venta", IntegerType(), True),
    StructField("fecha", StringType(), True),
    StructField("cliente", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("monto", DoubleType(), True)
])

df_ventas = spark.read.schema(schema_ventas).option("header", "false").csv(ruta_ventas)

df_ciudades = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta_ciudades)

# ============================================================
# ETAPA 2: VALIDACIÓN DE ENTRADAS
# ============================================================
print("\n=== ETAPA 2: VALIDACIÓN DE ENTRADAS ===")

cantidad_ventas = df_ventas.count()
cantidad_ciudades = df_ciudades.count()

print("Cantidad de ventas:", cantidad_ventas)
print("Cantidad de ciudades:", cantidad_ciudades)

if cantidad_ventas == 0:
    raise ValueError("El pipeline no puede continuar: el dataset de ventas está vacío.")

if cantidad_ciudades == 0:
    raise ValueError("El pipeline no puede continuar: el dataset de ciudades está vacío.")

# ============================================================
# ETAPA 3: INTEGRACIÓN
# ============================================================
print("\n=== ETAPA 3: INTEGRACIÓN ===")

# DECISIÓN:
# Se utiliza LEFT JOIN para conservar todas las ventas originales,
# aunque su ciudad no exista en la referencia.
df_integrado = df_ventas.join(
    df_ciudades,
    on="ciudad",
    how="left"
)

cantidad_integrada = df_integrado.count()

print("\n=== VALIDACIÓN DEL JOIN ===")
print("Ventas antes del join:", cantidad_ventas)
print("Registros después del join:", cantidad_integrada)

# Supone que la referencia tiene una única fila por ciudad
if cantidad_integrada != cantidad_ventas:
    raise ValueError(
        "Validación fallida: la cantidad de registros "
        "cambió después del left join."
    )

df_sin_region = df_integrado.filter(col("region").isNull())

cantidad_sin_region = df_sin_region.count()

print("Ventas sin información regional:", cantidad_sin_region)

# ============================================================
# ETAPA 4: TRANSFORMACIÓN
# ============================================================
print("\n=== ETAPA 4: TRANSFORMACIÓN ===")

# DECISIÓN:
# nivel_venta es una variable derivada: no existe en la fuente,
# se calcula a partir de monto.
df_transformado = df_integrado.withColumn(
    "nivel_venta",
    when(col("monto") < 100000, "BAJA")
    .when(
        (col("monto") >= 100000) &
        (col("monto") < 300000),
        "MEDIA"
    )
    .otherwise("ALTA")
)

print("\n=== DATOS TRANSFORMADOS ===")

df_transformado.select(
    "id_venta",
    "ciudad",
    "region",
    "categoria",
    "monto",
    "nivel_venta"
).show(20, False)

# ============================================================
# ETAPA 5: AGREGACIÓN
# ============================================================
print("\n=== ETAPA 5: AGREGACIÓN ===")

df_resultado = df_transformado \
    .groupBy(
        "region",
        "nivel_venta"
    ) \
    .agg(
        count("*").alias("cantidad_ventas"),
        round(sum("monto"), 2).alias("monto_total"),
        round(avg("monto"), 2).alias("monto_promedio")
    )

df_resultado = df_resultado.orderBy(
    "region",
    "nivel_venta"
)

print("\n=== RESULTADO ANALÍTICO ===")

df_resultado.show(100, False)

cantidad_resultados = df_resultado.count()

print("Filas del resultado analítico:", cantidad_resultados)

spark.stop()
EOF
```

## 2. Ejecute

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 pipeline_ventas.py 2>/dev/null
```

## 3. Resultado esperado

```text
Ventas antes del join: 100000
Registros después del join: 100000
Ventas sin información regional: 37338
Filas del resultado analítico: 18
```

| region | nivel_venta | cantidad_ventas | monto_total | monto_promedio |
|---|---|---:|---:|---:|
| **null** | ALTA | 29.910 | 26.918.598.046,41 | 899.986,56 |
| **null** | BAJA | 2.434 | 129.026.616,73 | 53.010,11 |
| **null** | MEDIA | 4.994 | 996.623.487,49 | 199.564,17 |
| Antofagasta | ALTA | 10.126 | 9.141.715.130,36 | 902.796,28 |
| Antofagasta | BAJA | 765 | 41.211.335,71 | 53.871,03 |
| Antofagasta | MEDIA | 1.723 | 344.761.946,93 | 200.093,99 |
| Araucania | ALTA | 9.925 | 8.973.226.679,41 | 904.103,44 |
| Araucania | BAJA | 784 | 41.114.415,35 | 52.441,86 |
| Araucania | MEDIA | 1.684 | 336.723.963,71 | 199.954,85 |
| Biobio | ALTA | 9.955 | 8.975.491.224,33 | 901.606,35 |
| Biobio | BAJA | 814 | 42.082.398,58 | 51.698,28 |
| Biobio | MEDIA | 1.622 | 326.515.916,49 | 201.304,51 |
| Metropolitana | ALTA | 10.207 | 9.127.549.371,42 | 894.244,08 |
| Metropolitana | BAJA | 810 | 41.399.543,72 | 51.110,55 |
| Metropolitana | MEDIA | 1.675 | 333.683.658,21 | 199.214,12 |
| Valparaiso | ALTA | 10.054 | 9.076.454.752,77 | 902.770,51 |
| Valparaiso | BAJA | 813 | 42.878.906,56 | 52.741,58 |
| Valparaiso | MEDIA | 1.705 | 343.916.446,27 | 201.710,53 |

> **18 filas** = 6 valores de región (5 regiones + `null`) × 3 niveles. Coquimbo y Los Lagos **no aparecen** porque sus ciudades no coincidieron en el `join`.

## 4. ¿Qué ocurre con las ventas sin región?

```text
registro sin región
        │
        ├── eliminarlo
        ├── asignar "DESCONOCIDA"
        ├── corregir la fuente
        └── mantener NULL
```

En este ejercicio **no se corrige**: primero se detecta y se comprende.

## 5. Analice

**a.** ¿Por qué `left join` y no `inner join`?

> Para conservar las 100.000 ventas. Un `inner join` habría eliminado 37.338 ventas (las de La_Serena, Puerto_Montt y Rancagua).

**b.** ¿Por qué verificar la cantidad de registros inmediatamente después del `join`?

> Porque un `join` puede perder registros (inner) o duplicarlos (claves repetidas en la referencia). Si la cantidad cambia, el resultado no es confiable.

**c.** ¿Qué diferencia hay entre conservar las 100.000 ventas y que todas tengan región?

> Conservarlas indica que no se perdieron filas; tener región indica que se pudieron enriquecer. Aquí se conservaron las 100.000, pero 37.338 quedaron sin región.

**d.** ¿Qué función cumple `nivel_venta`? ¿Es original o derivada?

> Clasifica cada venta en BAJA, MEDIA o ALTA según su monto. Es **derivada**: no existe en el archivo de origen.

**e.** ¿Qué representa una fila de `df_transformado`?

> Una venta individual enriquecida con región, zona y nivel.

**f.** ¿Qué representa una fila de `df_resultado`?

> Una combinación región + nivel de venta, con su cantidad, total y promedio.

**g.** ¿Por qué `df_resultado` tiene muchas menos filas?

> Porque la agregación resume 100.000 ventas en 18 combinaciones: cambia la unidad de análisis.

**h.** ¿Por qué no eliminar sin más los registros con `region = NULL`?

> Representan 37.338 ventas (37,34 %) y un monto importante. La causa es un problema de calidad de datos en la fuente (nombres que no coinciden), que debe corregirse en origen, no ocultarse.

## Concepto clave

```text
INTEGRAR (join) → TRANSFORMAR (withColumn) → AGREGAR (groupBy + agg)
```

---

# Ejercicio 13 — Persistir el producto analítico del pipeline en HDFS

# Ejercicio 14 — Ejecutar y validar el pipeline batch completo

> Ambos ejercicios usan la **versión final** de `pipeline_ventas.py`: el Ejercicio 13 incorpora la persistencia y el Ejercicio 14 ejecuta y valida el pipeline completo con mensajes por etapa.

## 1. Versión final del pipeline

📍 **spark-master**

```bash
cd /tmp/curso_spark/nivel3 && cat > pipeline_ventas.py <<'EOF'
# ============================================================
# PIPELINE BATCH DE VENTAS — Guía Práctica 3 (Ejercicios 11–14)
# Entradas : HDFS (ventas_hive.csv + ciudades.csv)
# Salida   : HDFS (Parquet) /curso/spark/guia3/pipeline/ventas_por_region_nivel
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    when,
    count,
    sum,
    avg,
    round
)

spark = SparkSession.builder \
    .appName("PipelineVentasGuia3") \
    .getOrCreate()

ruta_ventas = "hdfs://namenode:8020/curso/hive/datos_ventas/ventas_hive.csv"
ruta_ciudades = "hdfs://namenode:8020/curso/spark/guia3/fuentes/ciudades.csv"
ruta_salida = "hdfs://namenode:8020/curso/spark/guia3/pipeline/ventas_por_region_nivel"

# ============================================================
print("\n=== ETAPA 1: CARGA DE FUENTES ===")
# ============================================================
schema_ventas = StructType([
    StructField("id_venta", IntegerType(), True),
    StructField("fecha", StringType(), True),
    StructField("cliente", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("monto", DoubleType(), True)
])

df_ventas = spark.read.schema(schema_ventas).option("header", "false").csv(ruta_ventas)
df_ciudades = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta_ciudades)

# ============================================================
print("\n=== ETAPA 2: VALIDACIÓN DE ENTRADAS ===")
# ============================================================
cantidad_ventas = df_ventas.count()
cantidad_ciudades = df_ciudades.count()

print("Cantidad de ventas:", cantidad_ventas)
print("Cantidad de ciudades:", cantidad_ciudades)

if cantidad_ventas == 0:
    raise ValueError("El pipeline no puede continuar: el dataset de ventas está vacío.")
if cantidad_ciudades == 0:
    raise ValueError("El pipeline no puede continuar: el dataset de ciudades está vacío.")

# ============================================================
print("\n=== ETAPA 3: INTEGRACIÓN ===")
# ============================================================
df_integrado = df_ventas.join(df_ciudades, on="ciudad", how="left")

cantidad_integrada = df_integrado.count()
print("Ventas antes del join:", cantidad_ventas)
print("Registros después del join:", cantidad_integrada)

if cantidad_integrada != cantidad_ventas:
    raise ValueError("Validación fallida: la cantidad de registros cambió después del left join.")

cantidad_sin_region = df_integrado.filter(col("region").isNull()).count()
print("Ventas sin información regional:", cantidad_sin_region)

# ============================================================
print("\n=== ETAPA 4: TRANSFORMACIÓN ===")
# ============================================================
df_transformado = df_integrado.withColumn(
    "nivel_venta",
    when(col("monto") < 100000, "BAJA")
    .when((col("monto") >= 100000) & (col("monto") < 300000), "MEDIA")
    .otherwise("ALTA")
)

df_transformado.select("id_venta", "ciudad", "region", "categoria", "monto", "nivel_venta").show(10, False)

# ============================================================
print("\n=== ETAPA 5: AGREGACIÓN ===")
# ============================================================
df_resultado = df_transformado \
    .groupBy("region", "nivel_venta") \
    .agg(
        count("*").alias("cantidad_ventas"),
        round(sum("monto"), 2).alias("monto_total"),
        round(avg("monto"), 2).alias("monto_promedio")
    ) \
    .orderBy("region", "nivel_venta")

df_resultado.show(100, False)

# ============================================================
print("\n=== ETAPA 6: PERSISTENCIA ===")
# ============================================================
# DECISIÓN:
# Parquet conserva el esquema, es columnar y Spark lo lee directamente.
# overwrite permite re-ejecutar el pipeline reemplazando solo esta salida.
df_resultado.write \
    .mode("overwrite") \
    .parquet(ruta_salida)

print("Resultado almacenado correctamente en:", ruta_salida)

# ============================================================
print("\n=== ETAPA 7: VALIDACIÓN FINAL ===")
# ============================================================
df_validacion = spark.read.parquet(ruta_salida)

cantidad_original_resultado = df_resultado.count()
cantidad_resultado_persistido = df_validacion.count()

print("Filas antes de persistir:", cantidad_original_resultado)
print("Filas recuperadas desde HDFS:", cantidad_resultado_persistido)

if cantidad_original_resultado != cantidad_resultado_persistido:
    raise ValueError("Validación fallida: el resultado recuperado no contiene la cantidad esperada de filas.")

print("\n=== PIPELINE FINALIZADO CORRECTAMENTE ===")

spark.stop()
EOF
```

## 2. Ejecute el pipeline completo

```bash
/spark/bin/spark-submit --master spark://spark-master:7077 pipeline_ventas.py 2>/dev/null
```

## 3. Resultado esperado

```text
=== ETAPA 1: CARGA DE FUENTES ===
=== ETAPA 2: VALIDACIÓN DE ENTRADAS ===
Cantidad de ventas: 100000
Cantidad de ciudades: 7
=== ETAPA 3: INTEGRACIÓN ===
Ventas antes del join: 100000
Registros después del join: 100000
Ventas sin información regional: 37338
=== ETAPA 4: TRANSFORMACIÓN ===
(10 ventas con su nivel_venta)
=== ETAPA 5: AGREGACIÓN ===
(tabla de 18 filas: la misma del Ejercicio 12)
=== ETAPA 6: PERSISTENCIA ===
Resultado almacenado correctamente en: hdfs://namenode:8020/curso/spark/guia3/pipeline/ventas_por_region_nivel
=== ETAPA 7: VALIDACIÓN FINAL ===
Filas antes de persistir: 18
Filas recuperadas desde HDFS: 18
=== PIPELINE FINALIZADO CORRECTAMENTE ===
```

## 4. Verifique físicamente la salida en HDFS

📍 **Instancia EC2** (`exit` para salir de `spark-master`, o en otra ventana):

```bash
sudo docker exec namenode hdfs dfs -ls /curso/spark/guia3/pipeline/
```

```bash
sudo docker exec namenode hdfs dfs -ls /curso/spark/guia3/pipeline/ventas_por_region_nivel/
```

Resultado esperado:

```text
.../ventas_por_region_nivel/_SUCCESS
.../ventas_por_region_nivel/part-00000-....snappy.parquet
...
```

```bash
sudo docker exec namenode hdfs dfs -du -s -h /curso/spark/guia3/pipeline/ventas_por_region_nivel
```

## 5. Recupere el producto desde una nueva sesión Spark

📍 **spark-master**

```bash
cd /spark/bin && ./pyspark --master spark://spark-master:7077
```

📍 **PySpark (`>>>`)**

```python
df_recuperado = spark.read.parquet("hdfs://namenode:8020/curso/spark/guia3/pipeline/ventas_por_region_nivel")
```

```python
df_recuperado.printSchema()
```

```python
df_recuperado.show(100, False)
```

```python
print("Cantidad de filas recuperadas:", df_recuperado.count())
```

Resultado esperado: el esquema con `region`, `nivel_venta`, `cantidad_ventas`, `monto_total` y `monto_promedio`; **18 filas**.

```python
exit()
```

> El producto **sobrevive** a la aplicación que lo creó: puede ser la entrada de otro proceso.

## 6. Analice — Ejercicio 13

**a.** ¿Por qué `df_resultado` por sí solo no es un producto persistente?

> Porque existe solo en memoria mientras la aplicación está activa. Al ejecutar `spark.stop()`, desaparece.

**b.** ¿Por qué Parquet?

> Conserva el esquema y los tipos, es columnar (lee solo las columnas necesarias), ocupa menos espacio y Spark lo lee directamente sin `header` ni `inferSchema`.

**c.** ¿Qué función cumple `.mode("overwrite")`?

> Permite re-ejecutar el pipeline reemplazando el resultado anterior en esa ruta. Sin él, la segunda ejecución falla con `path already exists`.

**d.** ¿Por qué la salida tiene varios `part-*`?

> Cada partición del DataFrame se escribe como un archivo independiente, en paralelo. La salida es un **directorio**, no un archivo único.

**e.** ¿Diferencia entre `cache()`/`persist()` y escribir Parquet en HDFS?

> `cache()`/`persist()` conservan datos **temporalmente** durante la aplicación; desaparecen al terminarla. `write.parquet()` crea un producto **permanente** en HDFS, recuperable por otras aplicaciones.

**f.** ¿Por qué volver a leer el resultado después de escribirlo?

> Para comprobar que la escritura fue correcta y que el producto puede recuperarse.

**g.** ¿Qué demuestra recuperarlo desde una nueva sesión?

> Que el producto es independiente de la aplicación que lo creó: está persistido en HDFS.

**h.** ¿Por qué comparar cantidad de filas es útil pero no suficiente?

> Detecta pérdidas o duplicaciones, pero dos conjuntos con igual cantidad de filas pueden tener valores distintos. Habría que comparar también los totales o promedios.

## 7. Analice — Ejercicio 14

**a.** ¿Cuáles son las dos fuentes de entrada?

> `ventas_hive.csv` y `ciudades.csv`, ambas en HDFS.

**b.** ¿Cuál es el producto persistente generado?

> `ventas_por_region_nivel` en Parquet: cantidad, total y promedio por región y nivel de venta.

**c.** ¿Qué etapas recorre una venta?

> Lectura → validación → `left join` (recibe región y zona) → `withColumn` (recibe `nivel_venta`) → `groupBy` + `agg` (se resume en una combinación) → Parquet en HDFS.

**d.** ¿Por qué validar en distintas etapas?

> Para detectar cada problema donde ocurre: fuentes vacías (entrada), pérdida o duplicación (join), escritura incorrecta (salida).

**e.** ¿Diferencia entre `pipeline_ventas.py` y el producto Parquet?

> El `.py` es el **proceso** (las instrucciones); el Parquet es el **producto** (los datos resultantes). El programa termina; el producto permanece.

**f.** ¿Por qué es procesamiento **batch**?

> Porque procesa un conjunto de datos **ya acumulado y completo en HDFS**, como una unidad, en una sola ejecución que entrega un resultado.

**g.** ¿Qué condición deben cumplir los datos antes de ejecutar el pipeline?

> Deben estar **ya disponibles y almacenados en HDFS** cuando la aplicación comienza.

**h.** Si se ejecutara cada noche con las ventas del día, ¿seguiría siendo batch?

> Sí. Cada ejecución procesa un lote acumulado (las ventas del día) después de que fue almacenado. La frecuencia no cambia el modelo.

## Concepto clave

```text
DATOS ACUMULADOS (HDFS)
        ↓
PROCESAMIENTO (Spark)
        ↓
RESULTADO PERSISTENTE (HDFS)
```

Esto es **batch**.

---

# Ejercicio 15 — Del procesamiento batch al procesamiento continuo (conceptual)

| Característica | Pipeline batch (Spark) | Procesamiento continuo |
|---|---|---|
| Llegada de datos | Acumulados | Generados continuamente |
| Unidad observada | Lote | Evento |
| Inicio del procesamiento | Datos previamente disponibles en HDFS | Los eventos aparecen a medida que ocurren |
| Fuente | HDFS | Productor |
| Intermediación | No requerida | Necesaria |
| Tecnología | HDFS + Spark | Apache Kafka |

| Concepto | Definición |
|---|---|
| **Evento** | Dato generado como consecuencia de algo que ocurre en un sistema (una venta) |
| **Productor** | Sistema que publica eventos (el sistema de caja de cada sucursal) |
| **Topic** | Canal lógico donde se publican eventos relacionados (`ventas`) |
| **Consumidor** | Sistema que recibe y procesa los eventos (un sistema de reportes o alertas) |

```text
PRODUCTOR (caja de sucursal) → publica → TOPIC "ventas" → entrega → CONSUMIDOR (sistema de reportes)
```

> **Kafka no reemplaza a Spark.** HDFS almacena, Spark procesa y Kafka publica y entrega eventos. Pueden formar parte de la misma arquitectura.

---

# Anexo A — Crear `ciudades.csv` en HDFS (solo si falta)

📍 **Instancia EC2**

```bash
printf 'ciudad,region,zona\nSantiago,Metropolitana,Centro\nValparaiso,Valparaiso,Centro\nConcepcion,Biobio,Sur\nLa Serena,Coquimbo,Norte\nPuerto Montt,Los Lagos,Sur\nTemuco,Araucania,Sur\nAntofagasta,Antofagasta,Norte\n' > ~/ciudades.csv && cat ~/ciudades.csv
```

```bash
sudo docker cp ~/ciudades.csv namenode:/tmp/ciudades.csv
```

```bash
sudo docker exec namenode bash -c "hdfs dfs -mkdir -p /curso/spark/guia3/fuentes && hdfs dfs -put -f /tmp/ciudades.csv /curso/spark/guia3/fuentes/"
```

```bash
sudo docker exec namenode hdfs dfs -cat /curso/spark/guia3/fuentes/ciudades.csv
```

---

# Anexo B — Resumen de comandos

```text
PASO 0   ssh -i $HOME\.ssh\aws_hadoop -L 8080:localhost:8080 -L 50070:localhost:50070 ubuntu@IP_PUBLICA
         sudo docker exec namenode hdfs dfs -ls -R /curso | grep -E "ventas_hive.csv|ciudades.csv"
         sudo docker exec -it spark-master bash
         export PYSPARK_PYTHON=python3 && export PYSPARK_DRIVER_PYTHON=python3 && export PYTHONIOENCODING=utf-8 && mkdir -p /tmp/curso_spark/nivel3 && cd /tmp/curso_spark

EJECUTAR /spark/bin/spark-submit --master spark://spark-master:7077 ARCHIVO.py 2>/dev/null
PYSPARK  cd /spark/bin && ./pyspark --master spark://spark-master:7077
HDFS     sudo docker exec namenode hdfs dfs -ls RUTA
```

| Archivo | Ubicación (spark-master) | Contenido |
|---|---|---|
| `sesion_spark.py` | `/tmp/curso_spark` | Parte 1: SparkSession |
| `ejercicio12.py` | `/tmp/curso_spark` | Parte 2: DataFrame API vs Spark SQL |
| `ejercicio16.py` | `/tmp/curso_spark` | Parte 3: primer programa |
| `ejercicio17.py` | `/tmp/curso_spark` | Parte 3: programa documentado |
| `pipeline_ventas.py` | `/tmp/curso_spark/nivel3` | Parte 4: pipeline completo |

---

# Anexo C — Errores frecuentes

| Mensaje | Causa | Solución |
|---|---|---|
| `Connection timed out` | IP incorrecta o laboratorio apagado | Paso 0.1 |
| `Permission denied (publickey)` | Llave no autorizada | Autorizar `aws_hadoop.pub` en `~/.ssh/authorized_keys` |
| `Path does not exist` | Falta una fuente en HDFS | Paso 0.3 / Anexo A |
| `NameError: name 'x' is not defined` | Variable o función no definida o no importada | Revisar imports y nombres |
| `cannot resolve 'region'` | `ciudades.csv` leído sin `header` | `.option("header", "true")` |
| `path ... already exists` | Escritura sin `overwrite` | `.mode("overwrite")` |
| `Validación fallida: ... left join` | `ciudades.csv` tiene ciudades duplicadas | Revisar con `hdfs dfs -cat` |
| `SyntaxError` | Falta un paréntesis, una coma o una comilla | Revisar la línea indicada |
| Errores de Python 2 | Faltó el `export` | Paso 0.5 |
| `UnicodeEncodeError` al imprimir | Textos con tildes en un Python antiguo | `export PYTHONIOENCODING=utf-8` (Paso 0.5) |
| La aplicación queda en `WAITING` | Otra aplicación ocupa los cores | Cerrar otras sesiones `pyspark` (`exit()`) |
| No se ve la salida entre tantas líneas | Mensajes `INFO` | Agregar `2>/dev/null` al `spark-submit` |

---

# Anexo D — Conceptos clave

| Concepto | Definición breve |
|---|---|
| **HDFS** | Almacenamiento distribuido y persistente. Divide archivos en bloques. |
| **Spark** | Motor de procesamiento distribuido. |
| **SparkSession** | Punto de entrada a Spark: lee, crea DataFrames y ejecuta SQL. |
| **DataFrame** | Datos distribuidos con filas, columnas y schema. |
| **Schema** | Nombres y tipos de las columnas. |
| **Vista temporal** | Nombre para consultar un DataFrame con SQL; no es tabla Hive; desaparece al cerrar la sesión. |
| **Transformación** | Define el plan (`filter`, `groupBy`, `join`, `withColumn`); no ejecuta. |
| **Acción** | Ejecuta el plan (`show`, `count`, `write`). |
| **Lazy evaluation** | Spark no procesa hasta que una acción lo exige. |
| **DataFrame API vs Spark SQL** | Dos sintaxis, mismo plan, mismo motor, mismo resultado. |
| **Spark SQL vs HiveQL** | Sintaxis similar, motores distintos; solo comparten los datos en HDFS. |
| **`spark-submit`** | Ejecuta un `.py` completo en el clúster. |
| **Left join** | Conserva todos los registros de la izquierda; sin correspondencia → `NULL`. |
| **Variable derivada** | Columna calculada que no existía en la fuente (`nivel_venta`). |
| **Parquet** | Formato columnar, binario, con esquema incorporado. |
| **`part-*` / `_SUCCESS`** | Un archivo por partición / marca de escritura completada. |
| **`overwrite`** | Reemplaza la salida anterior en esa ruta. |
| **`cache` / `persist`** | Reutilización temporal en memoria durante la aplicación. |
| **Pipeline** | Secuencia reproducible: fuentes → validación → procesamiento → persistencia → validación. |
| **Batch** | Procesar un conjunto acumulado y almacenado, como un lote, en una ejecución. |
