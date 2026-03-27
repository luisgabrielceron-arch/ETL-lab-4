# ETL Lab 4 — Great Expectations  
## Avance documentado: Task A, Task B y Task C

## 1. Contexto del laboratorio

Este laboratorio consiste en diseñar e implementar un pipeline ETL para un escenario retail en Latinoamérica, incorporando validación de calidad de datos con Great Expectations, estrategias de limpieza y transformación, y una salida confiable para análisis de negocio. El dataset de entrada es `retail_etl_dataset.csv` y contiene problemas conocidos de completitud, unicidad, validez, consistencia, exactitud y oportunidad que deben identificarse, cuantificarse y corregirse a lo largo del pipeline. 

El objetivo general del laboratorio es construir un pipeline ETL completo que valide la calidad de los datos, aplique estrategias apropiadas de limpieza y transformación, y produzca información útil y confiable desde un modelo dimensional.

Los objetivos de negocio que guían el desarrollo son los siguientes:

- **BO-1 — Garantizar integridad financiera:** asegurar que los reportes de ingresos reflejen ventas reales, eliminando duplicados, negativos y totales incorrectos.
- **BO-2 — Soportar planeación estratégica con tendencias confiables:** estandarizar fechas y asegurar completitud para permitir análisis temporales.
- **BO-3 — Fortalecer análisis de producto y región:** estandarizar productos y países para identificar desempeño comercial con confianza.
- **BO-4 — Habilitar reportes transparentes y defendibles:** entregar datos limpios, validados y auditables.

---

# 3. Task A — Extract & Profiling

## 3.1. Qué pide la guía

La guía solicita cargar el dataset en un DataFrame de Pandas y registrarlo como un datasource en memoria para Great Expectations. Después, se debe realizar un profiling sistemático del dataset crudo para entender su estado antes de escribir expectativas. El script debe calcular y reportar:

- shape, tipos de dato y uso de memoria
- cantidad y porcentaje de nulos por columna
- cardinalidad de columnas categóricas
- estadísticas descriptivas de `quantity`, `price` y `total_revenue`
- número de valores duplicados en `invoice_id`
- cantidad de filas donde `total_revenue ≠ quantity × price` con tolerancia ±0.01
- distribución de formatos en `invoice_date`
- cantidad de fechas futuras y valores tipo nulo en `invoice_date` :contentReference[oaicite:4]{index=4}

El entregable requerido para esta fase es una **profiling summary table** dentro del reporte PDF. 

## 3.2. Qué se implementó

Se desarrolló el archivo `src/extract.py`, encargado de:

- cargar el CSV desde `data/raw/`
- generar profiling del dataset crudo
- registrar el DataFrame en Great Expectations
- exportar evidencia de profiling a la carpeta `reports/`

## 3.3. Resultados obtenidos

Durante la ejecución de `extract.py` se obtuvieron los siguientes hallazgos principales:

- El dataset contiene **5100 filas y 8 columnas**.
- `customer_id` fue inferido como `float64`, lo que sugiere la presencia de valores nulos.
- `invoice_date` permaneció como texto, lo que indica que aún no está normalizado como fecha.
- Se identificaron **7 países distintos**, aunque el catálogo esperado es más restringido.
- Se identificaron **5 tipos de formato de fecha**.
- Se detectaron valores negativos en `quantity` y `price`.
- Se detectaron **2131 filas involucradas en duplicidad de `invoice_id`**, con **1159 duplicados excedentes** y **972 IDs distintos repetidos**.
- Se detectaron **148 filas** donde `total_revenue` no coincide con `quantity × price`.
- Se detectaron **128 fechas futuras** y **13 valores null-like** en `invoice_date`.

## 3.4. Interpretación técnica

Los resultados del profiling muestran que el dataset raw no es apto para análisis directo. Existen problemas relevantes en varias dimensiones de calidad:

- **Completitud:** por nulos en `customer_id` y `invoice_date`
- **Unicidad:** por duplicados en `invoice_id`
- **Validez:** por cantidades y precios negativos
- **Exactitud:** por inconsistencias en `total_revenue`
- **Consistencia y oportunidad:** por países no estandarizados y fechas en formatos mezclados o fuera de rango

Esto confirma lo planteado por la guía, que anticipa valores faltantes, duplicados, nombres inconsistentes de país, negativos en cantidad y precio, y fechas mixtas o fuera de rango. 

## 3.5. Sustentación de la fase A

La fase A permite establecer una línea base objetiva del estado del dataset. Antes de limpiar o transformar, fue necesario medir la magnitud de los problemas para justificar las siguientes decisiones del pipeline. El profiling no corrige nada todavía; su objetivo es **diagnosticar** y dejar evidencia cuantitativa del problema. 

## 3.6. Evidencias generadas

Esta fase generó los siguientes archivos:

- `reports/profiling_summary.csv`
- `reports/profiling_overview.json`
- `reports/profiling_overview.md`

---

# 4. Task B — Input Data Validation (Great Expectations)

## 4.1. Qué pide la guía

La guía solicita construir una **Expectation Suite** para el dataset crudo. El propósito de esta suite es **medir y documentar los problemas de calidad**, por lo que la mayoría de las expectativas están diseñadas para **fallar**. Estas tasas de fallo constituyen la línea base de calidad del input. 

La suite debe incluir al menos una expectativa por dimensión de calidad, cubriendo:

- completitud en `customer_id` e `invoice_date`
- unicidad en `invoice_id`
- validez en `quantity`, `price` y `product`
- exactitud en `total_revenue`
- consistencia en `country`
- oportunidad / calidad temporal en `invoice_date` 

Los entregables requeridos para esta fase son:

- **Expectation Suite JSON**
- **Data Docs HTML report**
- **Failure rate summary table** en el PDF 

## 4.2. Qué se implementó

Se desarrolló el archivo `src/validate_input.py`, encargado de:

- cargar el dataset raw
- definir una suite de 10 expectativas
- ejecutar la validación con Great Expectations
- guardar los resultados en JSON, CSV, Markdown, HTML y Data Docs

## 4.3. Expectations implementadas

Se implementaron las siguientes 10 reglas técnicas:

1. `customer_id` no debe ser nulo
2. `invoice_date` no debe ser nulo
3. `invoice_id` debe ser único
4. `quantity` debe ser mayor o igual a 1
5. `price` debe ser mayor o igual a 0.01
6. `product` debe pertenecer al catálogo permitido
7. `total_revenue` debe coincidir con `quantity × price` con tolerancia ±0.01
8. `country` debe pertenecer al conjunto válido
9. `invoice_date` debe cumplir el patrón `YYYY-MM-DD`
10. `invoice_date` debe estar dentro de 2023

## 4.4. Resultados obtenidos

La validación arrojó los siguientes resultados:

- **Número de expectations:** 10
- **Data Quality Score (input):** 0.0%

Tasas de fallo observadas:

- Accuracy — `total_revenue == quantity * price` → **2.90%**
- Completeness — `customer_id not null` → **3.96%**
- Completeness — `invoice_date not null` → **0.25%**
- Consistency — `country in valid set` → **43.35%**
- Timeliness — `invoice_date matches YYYY-MM-DD` → **3.50%**
- Timeliness — `invoice_date within 2023` → **FAIL** con porcentaje no calculado automáticamente
- Uniqueness — `invoice_id unique` → **41.78%**
- Validity — `price >= 0.01` → **1.98%**
- Validity — `product in allowed catalog` → **12.65%**
- Validity — `quantity >= 1` → **2.92%**

## 4.5. Interpretación técnica

El hecho de que todas las expectations fallen no representa un error del proceso, sino un resultado esperado en esta fase. La guía establece que la suite de entrada debe funcionar como una **línea base de calidad**, no como un contrato de aprobación. :contentReference[oaicite:11]{index=11}

Los resultados muestran que los problemas más severos del raw dataset están asociados a:

- **consistencia de país**
- **unicidad de factura**
- **control de catálogo de producto**
- **calidad temporal de fechas**

Esto refuerza la necesidad de una fase posterior de análisis, limpieza y transformación antes de cargar los datos a un modelo dimensional.

## 4.6. Sustentación de la fase B

La validación de entrada convierte problemas observados en profiling en **reglas formales y reproducibles**. Su valor está en que:

- cuantifica cada falla
- deja una línea base medible
- conecta problemas técnicos con objetivos de negocio
- deja evidencia auditable mediante JSON y Data Docs

Esta fase cumple con el objetivo de implementar reglas de calidad con Great Expectations y documentar los fallos del dataset crudo. 

## 4.7. Evidencias generadas

Esta fase generó los siguientes archivos:

- `gx_context/expectations/retail_input_validation_suite.json`
- `reports/input_validation_results.json`
- `reports/input_validation_failure_summary.csv`
- `reports/input_validation_failure_summary.md`
- `reports/input_validation_failure_summary.html`
- `gx_context/uncommitted/data_docs/local_site/index.html`

---

# 5. Task C — Data Quality Analysis and Policy Proposal

## 5.1. Qué pide la guía

La guía solicita producir dos tablas:

1. **Data Quality Issues Table**
2. **Data Quality Policy Proposal** 

La **tabla de issues** debe contener:

- columna
- problema
- ejemplo
- dimensión
- impacto en el negocio 

La **tabla de políticas** debe contener al menos 8 políticas e incluir:

- enunciado de la política
- expectation de Great Expectations
- severidad
- objetivo(s) de negocio que cubre

Las primeras 6 políticas son base según la guía, y se deben agregar al menos 2 políticas originales. 

## 5.2. Qué se desarrolló

Con base en los resultados de profiling y validación de entrada, se construyeron:

- una tabla de **problemas de calidad**
- una tabla de **políticas de calidad**

Estas tablas conectan el diagnóstico técnico con la gobernanza del pipeline.

## 5.3. Tabla de issues de calidad

![alt text](image.png)

## 5.4. Tabla de políticas de calidad

![alt text](image-1.png)

## 5.5. Interpretación técnica

La fase C convierte los hallazgos técnicos en decisiones de gobierno de datos. Mientras la fase A diagnostica y la fase B mide fallos formalmente, la fase C permite:

- clasificar cada issue dentro de una dimensión de calidad
- explicar su impacto en el negocio
- establecer reglas que deben gobernar el pipeline

Esta etapa es importante porque conecta los problemas observados con los objetivos BO-1 a BO-4 y justifica por qué ciertas validaciones deben mantenerse también en fases posteriores. 

## 5.6. Sustentación de la fase C

La fase C no corrige todavía los datos, pero sí prepara la lógica de control que va a sostener las siguientes etapas. Las políticas formuladas en esta fase sirven como base para:

- justificar decisiones de limpieza en Task D
- validar resultados después de transformación en Task F
- asegurar trazabilidad y auditabilidad del pipeline

Esto se alinea con el resultado de aprendizaje de conectar decisiones de calidad con resultados medibles del negocio. 

---

# 6. Task D — Cleaning

## 6.1. Qué pide la guía

La etapa de **Cleaning** consiste en corregir o remover registros rotos o inválidos. La guía establece que un registro que falle esta etapa **no puede entrar de forma segura al pipeline**, y que cada decisión debe justificarse. También exige tratar explícitamente estos problemas: `duplicate invoice_id`, `negative quantity`, `negative price`, `NULL customer_id`, `null-like invoice_date`, `future invoice_date` e `inaccurate total_revenue`. Al finalizar, se debe imprimir un resumen **before/after** con conteo de filas, nulos por columna y número de filas afectadas por cada razón, y guardar el dataset limpio como `data/processed/retail_clean.csv`. :contentReference[oaicite:0]{index=0}

## 6.2. Objetivo de esta etapa

El objetivo de esta fase es dejar el dataset en un estado **seguro y confiable** antes de pasar a transformación. A diferencia de la etapa E, donde ya no se eliminan filas, aquí sí se permite descartar registros inválidos o corregir campos derivados cuando eso esté justificado. Esta fase soporta principalmente los objetivos de negocio **BO-1, BO-2 y BO-3**, al eliminar duplicados, negativos, fechas inválidas y errores financieros antes de producir datos analíticos. 

## 6.3. Estrategias de limpieza aplicadas

Con base en la guía y en los hallazgos de las fases A, B y C, se aplicaron las siguientes acciones:

![alt text](image.png)

## 6.4. Resultados obtenidos

La ejecución de `clean.py` produjo el siguiente resultado:

- **Row count before:** 5100  
- **Row count after:** 3500  
- **Rows dropped total:** 1600  
- **Rows corrected total:** 115  

### Nulos antes de cleaning
- `invoice_id`: 0
- `customer_id`: 202
- `product`: 0
- `quantity`: 0
- `price`: 0
- `total_revenue`: 0
- `country`: 0
- `invoice_date`: 13

### Nulos después de cleaning
- `invoice_id`: 0
- `customer_id`: 0
- `product`: 0
- `quantity`: 0
- `price`: 0
- `total_revenue`: 0
- `country`: 0
- `invoice_date`: 0

### Filas afectadas por cada razón
- `duplicate_invoice_id`: 1159 filas eliminadas
- `negative_or_invalid_quantity`: 120 filas eliminadas
- `negative_or_invalid_price`: 74 filas eliminadas
- `null_customer_id`: 144 filas eliminadas
- `null_like_invoice_date`: 8 filas eliminadas
- `unparseable_invoice_date`: 2 filas eliminadas
- `future_invoice_date`: 93 filas eliminadas
- `inaccurate_total_revenue`: 115 filas corregidas

## 6.5. Interpretación de los resultados

El proceso de limpieza redujo el dataset de **5100** a **3500** filas, eliminando registros que no podían ingresar de manera segura al pipeline y corrigiendo errores en un campo derivado. El mayor impacto provino de los duplicados en `invoice_id`, lo cual es consistente con el problema de unicidad descrito en la guía. También se eliminaron registros con cantidades o precios inválidos, nulos en `customer_id`, fechas nulas, fechas no parseables y fechas futuras. Esto está alineado con los problemas conocidos del dataset raw: duplicados, negativos, nulos, inconsistencias temporales y errores en `total_revenue`. :contentReference[oaicite:3]{index=3}

Es importante aclarar que los conteos de cleaning no siempre coinciden exactamente con los conteos del profiling inicial. Esto ocurre porque el cleaning se aplica **de forma secuencial**: una fila eliminada por duplicidad ya no llega a evaluarse en reglas posteriores como `customer_id` nulo o `future_invoice_date`. Por eso, los totales de esta etapa reflejan el número de filas realmente afectadas en cada paso, no necesariamente el total bruto detectado al comienzo.

## 6.6. Sustentación técnica

La estrategia aplicada distingue entre **registros irrecuperables** y **campos corregibles**. Los registros con errores estructurales o semánticos graves fueron eliminados, porque habrían contaminado los análisis financieros, temporales o comerciales. En cambio, `total_revenue` se corrigió mediante recálculo, ya que se trata de un campo derivado y no de una captura primaria. Esta decisión preserva transacciones válidas sin sacrificar integridad analítica. La guía pide justamente justificar cada decisión y documentar las acciones de limpieza con su evidencia before/after. :contentReference[oaicite:4]{index=4}

## 6.7. Evidencias generadas

Esta fase generó los siguientes archivos:

- `data/processed/retail_clean.csv`
- `reports/cleaning_actions.csv`
- `reports/cleaning_actions.md`
- `reports/cleaning_before_after_summary.json`
- `reports/cleaning_before_after_summary.md`

Estos artefactos cubren el entregable de **“Cleaning Actions with justification and before/after summary”** requerido para el reporte PDF final y dejan el proyecto listo para avanzar a **Task E — Transformation**. 

# 7. Task E — Transformation

## 7.1. Qué pide la guía

La etapa de **Transformation** tiene como objetivo reestructurar los datos válidos para prepararlos para el modelo analítico. A diferencia de la etapa D, esta fase **no elimina filas**: se ejecuta sobre todos los registros que sobrevivieron al cleaning. La guía solicita aplicar estas transformaciones:

- estandarizar `country`
- parsear `invoice_date`
- extraer `year`, `month` y `day_of_week`
- convertir `customer_id` a `Int64`
- normalizar `product`
- crear `revenue_bin` con las categorías `Low`, `Medium` y `High`
- guardar el resultado como `data/processed/retail_transformed.csv` :contentReference[oaicite:0]{index=0}

Además, la guía indica que este archivo transformado alimenta tanto la **validación post-transformación (Task f)** como el **modelo dimensional (Task g)**. 

## 7.2. Objetivo de esta etapa

El objetivo de esta fase es dejar el dataset limpio en un formato homogéneo, enriquecido y analíticamente útil. Mientras que Cleaning se enfoca en remover o corregir registros inseguros, Transformation se enfoca en **estandarizar y derivar columnas** para soportar análisis temporal, segmentación por país y clasificación por nivel de ingresos. Esta etapa está ligada principalmente a los objetivos de negocio **BO-2** y **BO-3**. :contentReference[oaicite:2]{index=2}

## 7.3. Transformaciones aplicadas

Con base en la guía, se desarrolló `src/transform.py` y se aplicaron las siguientes transformaciones sobre `retail_clean.csv`:

| Transformación | Acción aplicada | Justificación |
|---|---|---|
| `country` | Estandarización mediante diccionario de mapeo a valores homogéneos | Permite análisis regional consistente |
| `invoice_date` | Conversión de texto a `datetime` | Habilita análisis temporal y derivación de calendario |
| `year`, `month`, `day_of_week` | Extracción desde `invoice_date` | Soporta análisis por año, mes y día de la semana |
| `customer_id` | Conversión a `Int64` | Ajusta el tipo al esperado por el modelo analítico |
| `product` | Normalización con `strip()` y `title()` | Elimina diferencias de formato en nombres de producto |
| `revenue_bin` | Segmentación de `total_revenue` en `Low`, `Medium`, `High` usando cuantiles | Agrega una característica útil para análisis exploratorio |

Estas acciones coinciden con lo exigido por la guía para la fase E. :contentReference[oaicite:3]{index=3}

## 7.4. Resultados obtenidos

La ejecución de `transform.py` produjo el siguiente resultado:

- **Row count before:** 3500  
- **Row count after:** 3500  

Esto confirma que no se eliminaron filas durante la transformación, tal como lo exige la guía. :contentReference[oaicite:4]{index=4}

### Columnas finales del dataset transformado
- `invoice_id` (`Int64`)
- `customer_id` (`Int64`)
- `product` (`string`)
- `quantity` (`Int64`)
- `price` (`float64`)
- `total_revenue` (`float64`)
- `country` (`string`)
- `invoice_date` (`datetime64[us]`)
- `year` (`Int64`)
- `month` (`Int64`)
- `day_of_week` (`string`)
- `revenue_bin` (`string`)

### Valores finales de `country`
- Chile
- Colombia
- Ecuador
- Peru

### Distribución de `revenue_bin`
- `Low`: 1167
- `Medium`: 1166
- `High`: 1167

### Distribución mensual
- Mes 1: 273
- Mes 2: 270
- Mes 3: 307
- Mes 4: 278
- Mes 5: 346
- Mes 6: 260
- Mes 7: 295
- Mes 8: 275
- Mes 9: 320
- Mes 10: 293
- Mes 11: 320
- Mes 12: 263

## 7.5. Interpretación de los resultados

El resultado de esta fase indica que el dataset ya quedó en una estructura apta para análisis. Se logró:

- mantener intacta la cantidad de filas
- convertir `invoice_date` a fecha real
- derivar columnas de calendario (`year`, `month`, `day_of_week`)
- homogeneizar el campo `country` al conjunto esperado
- tipar correctamente `customer_id`
- crear una segmentación de ingresos equilibrada mediante `revenue_bin`

La estandarización de `country` fue exitosa, ya que los valores finales coinciden con el conjunto esperado `{Colombia, Ecuador, Peru, Chile}`, el mismo que luego será validado en la etapa F. 

La distribución de `revenue_bin` es casi perfectamente balanceada, lo cual es coherente con una estrategia de binning basada en cuantiles. Esto hace que la nueva variable sea útil para análisis descriptivo y comparativo.

La distribución mensual también confirma que los datos transformados cubren los 12 meses del año, sin valores fuera de rango, lo cual refuerza la consistencia temporal lograda en las etapas D y E.

## 7.6. Sustentación técnica

La etapa E no busca corregir problemas estructurales del raw dataset, sino **reformatear y enriquecer** la información ya depurada. Esta separación entre Cleaning y Transformation es explícitamente requerida por la guía como parte del aprendizaje del laboratorio. :contentReference[oaicite:6]{index=6}

Las transformaciones aplicadas preparan el dataset para dos usos inmediatos:

1. **Task f — Post-Transformation Validation**, donde el dataset transformado debe comportarse como un contrato de calidad y pasar todas las expectativas. La guía indica que aquí deben validarse, entre otras cosas, `invoice_date` como datetime, `month` en `[1, 12]`, `country` solo en el conjunto permitido, `revenue_bin` solo en `{Low, Medium, High}`, `total_revenue > 0` e `invoice_id` único. :contentReference[oaicite:7]{index=7}

2. **Task g — Dimensional Modeling**, donde el dataset transformado servirá como fuente para construir dimensiones y tabla de hechos del esquema estrella. 

## 7.7. Evidencias generadas

Esta fase generó los siguientes archivos:

- `data/processed/retail_transformed.csv`
- `reports/transformation_summary.json`
- `reports/transformation_summary.md`

