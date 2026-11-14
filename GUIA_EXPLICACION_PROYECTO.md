# GUÍA MAESTRA DE ESTUDIO Y SUSTENTACIÓN: WORKSHOP-3
## Streaming ETL con Apache Kafka, Machine Learning y PostgreSQL
**Programa Académico:** Ingeniería de Datos e Inteligencia Artificial  
**Curso:** ETL (G01) — Semestre 2026-2  
**Ponderación de Evaluación:** Proyecto Técnico (70%) + Sustentación Oral Individual (30%)

---

## 1. MATRIZ DE CUMPLIMIENTO ESTRICTO (AUDITORÍA OFICIAL DEL PDF)

Esta matriz audita y mapea cada requerimiento innegociable extraído directamente de `ETL-G01_2026-2_Workshop-3.pdf`:

| Componente | Requisito del PDF | Estado | Implementación / Archivo |
| :--- | :--- | :---: | :--- |
| **Paso 0** | Renderizado multimodal a 300 DPI y extracción textual completa | COMPLETO | `docs/pdf_pages/page_01..12.png`, `docs/full_extracted_pdf.txt` |
| **Paso 1** | EDA de datasets 2015-2019: nulos, duplicados, discrepancias de columnas, tipos y outliers | COMPLETO | `notebooks/eda.ipynb` |
| **Paso 2** | Esquema unificado, estandarización de columnas, tipos y justificación de limpieza | COMPLETO | `src/harmonization.py` -> `data/processed/happiness_unified.csv` |
| **Paso 3** | Feature engineering sin target leakage, estadísticas descriptivas y correlaciones | COMPLETO | `notebooks/model_training.ipynb` |
| **Paso 4** | Train/Test Split 70/30, Pipeline scikit-learn con preprocesamiento, métricas MAE, RMSE, R² | COMPLETO | `notebooks/model_training.ipynb`, `models/model.pkl` |
| **Infra** | Docker Compose con Apache Kafka (3 particiones, factor 1) y PostgreSQL 16 | COMPLETO | `docker-compose.yml`, `sql/create_tables.sql` |
| **Paso 5** | Kafka Producer emitiendo eventos JSON a `happiness-events` con llave `country` | COMPLETO | `kafka/producer.py` |
| **Paso 6** | Kafka Consumer grupo `happiness-ml-team`, almacenamiento crudo previo y tolerancia a fallos | COMPLETO | `kafka/consumer.py` |
| **Paso 7** | Modelo dimensional Star Schema en PostgreSQL (`raw_happiness_events`, `dim_country`, `dim_date`, `fact_predictions`) | COMPLETO | `sql/create_tables.sql`, `src/verify_db.py` |
| **Paso 8** | Carga en PostgreSQL de eventos válidos e inválidos vinculados por `raw_event_id` | COMPLETO | `kafka/consumer.py`, verificación con queries |
| **Paso 9** | 4 KPIs analíticos ejecutados directamente sobre PostgreSQL | COMPLETO | `sql/kpis.sql`, `src/run_kpis.py`, `dashboards/` |
| **Entregables** | `README.md` (Inglés), `requirements.txt`, Guía Pedagógica de Sustentación (Español) | COMPLETO | `README.md`, `requirements.txt`, `GUIA_EXPLICACION_PROYECTO.md` |

---

## 2. ARQUITECTURA GENERAL DEL SISTEMA

El sistema se divide conceptualmente en dos grandes subsistemas coordinados:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        PROCESO OFFLINE (BATCH)                         │
└────────────────────────────────────────────────────────────────────────┘
  Datasets Históricos (2015.csv - 2019.csv en data/raw/)
            │
            ▼
  Auditoría y Perfilado de Datos (notebooks/eda.ipynb)
            │
            ▼
  Armonización y Estandarización (src/harmonization.py)
            │
            ▼
  Dataset Unificado (data/processed/happiness_unified.csv)
            │
            ▼
  Selección de Características (Sin Target Leakage) + Split 70/30
            │
            ▼
  Entrenamiento de Scikit-Learn Pipeline (Imputer + Scaler + Regressor)
            │
            ▼
  Evaluación Formal (MAE, RMSE, R²) y Serialización (models/model.pkl)
            │
            ▼
  Generación del Dataset de Streaming (data/streaming/happiness_stream_source.csv)

┌────────────────────────────────────────────────────────────────────────┐
│                      PROCESO ONLINE (STREAMING ETL)                     │
└────────────────────────────────────────────────────────────────────────┘
  Dataset Streaming (data/streaming/happiness_stream_source.csv)
            │
            ▼
  Kafka Producer (kafka/producer.py)
  - Emite JSON con delay configurable
  - Llave del mensaje = country
  - Tópico: happiness-events (3 particiones, factor 1)
  - Inyecta lote de eventos válidos y eventos sintéticos inválidos (test de resiliencia)
            │
            ▼
  Apache Kafka Broker (Confluent Platform en Docker Compose)
            │
            ▼
  Kafka Consumer (kafka/consumer.py) — Consumer Group: happiness-ml-team
            │
            ├─► 1. ATERRIZAJE INMEDIATO: Inserción en raw_happiness_events
            │      (Payload JSONB, metadata, estado inicial 'RECEIVED')
            │
            ├─► 2. VALIDACIÓN DEL CONTRATO DE EVENTOS:
            │      - Campos obligatorios ausentes -> Estado: INVALID_SCHEMA
            │      - Tipos de datos o valores ilógicos/nulos -> Estado: INVALID_VALUES
            │      - Errores de serialización -> Estado: PREDICTION_ERROR
            │      * Resiliencia: Los inválidos quedan registrados y NO tumban el pipeline.
            │
            └─► 3. INFERENCIA EN TIEMPO REAL (Para eventos con estado VALID):
                   - Upsert / lookup en dim_country y dim_date
                   - Inferencia con models/model.pkl (usando el pipeline scikit-learn)
                   - Cálculo del residuo/error: actual_score - predicted_score
                   - Inserción en fact_predictions (FK a raw_event_id, country_id, date_id)
            │
            ▼
  PostgreSQL 16 (Star Schema)
            │
            ▼
  Capa de Analítica y KPIs (sql/kpis.sql & dashboards/)
  - Query 1: Error Promedio de Predicción (MAE global y por país)
  - Query 2: Conteo de Predicciones por País
  - Query 3: Comparación Predicho vs Real
  - Query 4: Tendencia de Predicciones en el Tiempo
```

---

## 3. FASE 1: AUDITORÍA DE DATOS, EDA Y ARMONIZACIÓN DE ESQUEMAS

### 3.1. Hallazgos del Perfilado Exploratorio (2015 - 2019)
Al inspeccionar los 5 datasets anuales en `data/raw/` se identificaron las siguientes discrepancias estructurales:
1. **Volumen de Registros:** 
   - 2015: 158 filas, 12 columnas.
   - 2016: 157 filas, 13 columnas.
   - 2017: 155 filas, 12 columnas.
   - 2018: 156 filas, 9 columnas.
   - 2019: 156 filas, 9 columnas.
   - Total combinado: **782 observaciones**.
2. **Duplicados:** Exactamente 0 filas duplicadas en los 5 años.
3. **Valores Faltantes (NaN):** Solo existía **un único valor nulo** en todo el histórico: el país `United Arab Emirates` en 2018 carecía de `Perceptions of corruption`.
   - *Decisión técnica y justificación:* Dado que los Emiratos Árabes Unidos reportaron valores en 2015 (0.3858), 2016 (0.3556), 2017 (0.3245) y 2019 (0.1820), se realizó una imputación basada en la mediana histórica del propio país (0.3400), preservando la fidelidad de la serie temporal en lugar de descartar la fila o imputar la media global.

### 3.2. Tabla Maestra de Equivalencias de Columnas
Para cumplir con el **Contrato de Eventos** de Kafka y alimentar el modelo de Machine Learning, se construyó el siguiente mapeo unificado:

| Campo Armonizado (`snake_case`) | Tipo | 2015 | 2016 | 2017 | 2018 | 2019 | Rol en el Pipeline |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `country` | `VARCHAR` | `Country` | `Country` | `Country` | `Country or region` | `Country or region` | Llave de evento Kafka & Dimensión |
| `region` | `VARCHAR` | `Region` | `Region` | *Mapeado* | *Mapeado* | *Mapeado* | Atributo analítico enriquecido |
| `year` | `INT` | Inyectado (2015) | Inyectado (2016) | Inyectado (2017) | Inyectado (2018) | Inyectado (2019) | Partición temporal & Dimensión |
| `actual_happiness_score` | `FLOAT` | `Happiness Score` | `Happiness Score` | `Happiness.Score` | `Score` | `Score` | **Variable Objetivo (Solo Evaluación)** |
| `gdp` | `FLOAT` | `Economy (GDP per Capita)` | `Economy (GDP per Capita)` | `Economy..GDP.per.Capita.` | `GDP per capita` | `GDP per capita` | Feature 1 (PIB per cápita) |
| `family` | `FLOAT` | `Family` | `Family` | `Family` | `Social support` | `Social support` | Feature 2 (Soporte familiar/social) |
| `health` | `FLOAT` | `Health (Life Expectancy)` | `Health (Life Expectancy)` | `Health..Life.Expectancy.` | `Healthy life expectancy` | `Healthy life expectancy` | Feature 3 (Esperanza de vida) |
| `freedom` | `FLOAT` | `Freedom` | `Freedom` | `Freedom` | `Freedom to make life choices` | `Freedom to make life choices` | Feature 4 (Libertad de decisiones) |
| `generosity` | `FLOAT` | `Generosity` | `Generosity` | `Generosity` | `Generosity` | `Generosity` | Feature 5 (Generosidad reportada) |
| `corruption` | `FLOAT` | `Trust (Government Corruption)` | `Trust (Government Corruption)` | `Trust..Government.Corruption.` | `Perceptions of corruption` | `Perceptions of corruption` | Feature 6 (Confianza/ausencia corrupción) |

### 3.3. Prevención Estricta de Target Leakage (Fuga de Datos)
El PDF prohíbe categóricamente el uso de cualquier atributo que filtre información directa sobre la variable a predecir. Por ello, se descartaron deliberadamente:
1. `Happiness Rank` / `Overall rank`: Es el ranking derivado directamente de ordenar `Happiness Score`. Si se incluyera, el modelo no aprendería los factores socioeconómicos sino una trivialidad matemática. Además, en producción no se conoce el ranking antes de evaluar.
2. `Standard Error`, `Lower/Upper Confidence Interval`, `Whisker.high/low`: Intervalos de dispersión calculados directamente sobre la distribución de las respuestas del Score.
3. `Dystopia Residual`: En la metodología del World Happiness Report, la puntuación es exactamente igual a la suma de los 6 factores más el residuo de distopía. Incluir el residuo permitiría al modelo calcular una suma aritmética trivial, destruyendo la capacidad de generalización en un entorno real.

---

## 4. FASE 2: PIPELINE FORMAL DE MACHINE LEARNING (SCIKIT-LEARN)

### 4.1. Diseño del Pipeline y Encapsulación
Siguiendo las directivas estrictas de la rúbrica y las mejores prácticas de MLOps:
1. **Evitar Data Leakage en Preprocesamiento:** El escalado y la imputación se ajustan **únicamente sobre el 70% de entrenamiento** (`X_train`) y se aplican mediante transformación ciega sobre el 30% de prueba (`X_test`) y durante la inferencia en streaming.
2. **Encapsulamiento en `sklearn.pipeline.Pipeline`:**
   - **Paso 1: `SimpleImputer(strategy='median')`:** Otorga resiliencia en tiempo real en caso de que algún evento de streaming presente algún valor numérico nulo o ausente en sus features.
   - **Paso 2: `StandardScaler()`:** Estandariza la escala de las 6 variables predictoras ($z = (x - \mu)/\sigma$).
   - **Paso 3: `RandomForestRegressor(n_estimators=100, max_depth=10, min_samples_split=4, random_state=42)`:** Captura las complejas relaciones no lineales entre factores económicos (PIB), soporte social y expectativa de vida.

### 4.2. Métricas de Evaluación Obtenidas
Evaluación en el conjunto de prueba independiente (235 registros de prueba, 30%):

| Métrica | Definición / Interpretación | Valor en Test |
| :--- | :--- | :---: |
| **MAE** (Mean Absolute Error) | Error promedio en las unidades del target (0 a 10). | **0.4077** |
| **RMSE** (Root Mean Squared Error) | Penaliza errores grandes de predicción; sensible a anomalías. | **0.5239** |
| **$R^2$** (Coeficiente de Determinación) | Proporción de la varianza explicada por las 6 variables. | **0.7802** |

*Interpretación para la sustentación:* El modelo explica más del **78% de la variabilidad** de la felicidad mundial utilizando exclusivamente las 6 dimensiones socioeconómicas, con un error absoluto medio de apenas 0.4 puntos en una escala de 0 a 10.

### 4.3. Serialización e Inferencia Unitaria
El pipeline completo entrenado fue serializado en `models/model.pkl`.
Se implementó y verificó la función `predict_single_event(pipeline, event_dict)` en `src/train_model.py`:
- Entrada: Evento de prueba para `Colombia` con `actual_happiness_score = 6.2`.
- Inferencia: `6.2399`.
- Error de Predicción: `-0.0399` (precisión notable y congruente con el contrato de eventos).

---

## 5. FASE 3: INFRAESTRUCTURA DOCKER Y MODELO DIMENSIONAL EN POSTGRESQL (STAR SCHEMA)

### 5.1. Despliegue en Docker Compose
Se configuró un entorno reproducible con los siguientes servicios orquestados en `docker-compose.yml`:
1. **`zookeeper` (`confluentinc/cp-zookeeper:7.6.1`):** Coordinador de quórum y metadatos para Kafka, con healthcheck interno.
2. **`kafka` (`confluentinc/cp-kafka:7.6.1`):** Broker de mensajería configurado con listeners duales:
   - Interno: `PLAINTEXT://kafka:29092`
   - Externo (host): `PLAINTEXT_HOST://localhost:9092`
   - Parámetros de broker: `offsets.topic.replication.factor: 1`, `transaction.state.log.replication.factor: 1`.
3. **Tópico Kafka (`happiness-events`):**
   - **Particiones:** 3 particiones (garantiza paralelismo de consumidores y balanceo de carga por hash de la clave `country`).
   - **Factor de Replicación:** 1 (estándar para cluster local single-broker).
4. **`postgres` (`postgres:16-alpine`):**
   - Base de datos relacional analítica (`happiness_db`).
   - Mapeo de puerto en host: `6432:5432` (resolviendo de forma limpia la reserva de puertos de Hyper-V/NAT de Windows en el rango 5341-5440).
   - Inicialización automática montando `sql/create_tables.sql` en `/docker-entrypoint-initdb.d/`.

### 5.2. Diseño Relacional: Star Schema y Auditoría Cruda
El modelo dimensional implementado en `sql/create_tables.sql` separa estrictamente las fases de ingesta, normalización dimensional y hechos:

1. **Tabla de Aterrizaje Crudo (`raw_happiness_events`):**
   - `raw_event_id` (`BIGSERIAL PRIMARY KEY`): Identificador único de ingesta.
   - `event_id` (`VARCHAR(100)`): UUID / identificador del contrato de eventos.
   - `country`, `year`: Metadatos del evento.
   - `raw_payload` (`JSONB NOT NULL`): Copia idéntica del mensaje recibido antes de cualquier procesamiento.
   - `processing_status` (`VARCHAR(50)`): Estados posibles (`VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`, `PREDICTION_ERROR`).
   - `error_message` (`TEXT`): Registro del motivo de rechazo en caso de falla.
   - `ingestion_timestamp`: Marca temporal de recepción en el consumidor.
2. **Dimensión de Países (`dim_country`):**
   - `country_id` (`SERIAL PRIMARY KEY`).
   - `country_name` (`VARCHAR(100) UNIQUE NOT NULL`).
   - `region` (`VARCHAR(100)`).
3. **Dimensión Temporal (`dim_date`):**
   - `date_id` (`SERIAL PRIMARY KEY`).
   - `year` (`INT NOT NULL`), `event_time` (`TIMESTAMP NOT NULL`).
   - `month`, `day`, `hour`, `minute`.
   - Restricción única sobre `(year, event_time)`.
4. **Tabla de Hechos (`fact_predictions`):**
   - Clave primaria: `prediction_id` (`BIGSERIAL`).
   - Claves foráneas: `raw_event_id -> raw_happiness_events`, `country_id -> dim_country`, `date_id -> dim_date`.
   - Variables explicativas: `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`.
   - Medidas analíticas: `actual_score`, `predicted_score`, `prediction_error` ($Actual - Predicted$).
   - Marcas temporales: `event_time` (simulación), `prediction_timestamp` (tiempo real).

---

## 6. FASE 4: STREAMING ETL EN TIEMPO REAL CON KAFKA Y TOLERANCIA A FALLOS

### 6.1. Productor de Eventos (`kafka/producer.py`)
- Lee registros históricos uno a uno desde `data/streaming/happiness_stream_source.csv`.
- Emite eventos serializados en formato JSON cumpliendo el contrato oficial del PDF.
- Configura `country` como la **llave del mensaje** (`key=country.encode('utf-8')`), lo cual garantiza que todos los eventos de un mismo país vayan a la misma partición preservando el orden estricto de eventos por entidad.
- **Inyección de Pruebas de Fallo:** Al final de la transmisión histórica, inyecta 4 eventos sintéticos deliberadamente defectuosos:
  1. `ERR-SCHEMA-001`: Evento sin el campo obligatorio `gdp`.
  2. `ERR-VAL-002`: Evento con el campo `country` vacío.
  3. `ERR-VAL-003`: Evento con el campo `corruption` con texto en lugar de un número flotante.
  4. `ERR-SCHEMA-004`: Evento con una fecha no válida en formato ISO-8601.

### 6.2. Consumidor Resiliente (`kafka/consumer.py`)
- Grupo de consumo: `happiness-ml-team`.
- **Aterrizaje Inmediato:** Cada mensaje entrante se almacena primero en `raw_happiness_events` con estado `RECEIVED`.
- **Validación del Contrato:** Se valida la integridad del esquema y la plausibilidad de los tipos y rangos.
- **Tolerancia a Fallos Probada:**
  - Los 4 eventos malformados fueron capturados por el validador, marcados como `INVALID_SCHEMA` o `INVALID_VALUES` con el mensaje explícito del fallo en `error_message`, y omitidos de la inferencia de ML.
  - **El consumidor no se cayó en ningún momento**, logrando procesar el 100% de los 786 eventos.
- **Inferencia en Tiempo Real:** Para los 782 eventos válidos, el consumidor ejecutó la inferencia con `models/model.pkl`, calculó el error de predicción y realizó la inserción dimensional en `fact_predictions`.

---

## 7. FASE 5: RESULTADOS DE LOS 4 KPIS EN POSTGRESQL Y DASHBOARDS

Las consultas de `sql/kpis.sql` ejecutadas sobre PostgreSQL arrojaron los siguientes resultados analíticos oficiales:

### KPI 1: Error Promedio de Predicción
- **MAE Global:** `0.2427` puntos en la escala de felicidad.
- **RMSE Global:** `0.3434` puntos.
- **Sesgo Medio (Mean Bias):** `-0.0002` (el modelo es insesgado, sin tendencia sistemática a sobreestimar ni subestimar).
- **Desempeño Regional:**
  - *Regiones más predecibles:* Australia y Nueva Zelanda (MAE: `0.0786`), Norteamérica (MAE: `0.0880`), Europa Occidental (MAE: `0.1997`).
  - *Regiones con mayor variabilidad:* América Latina y el Caribe (MAE: `0.3128`), Asia del Sur (MAE: `0.3229`).

### KPI 2: Predicciones por País
Se procesaron observaciones completas para más de 150 naciones. Países de alta estabilidad macroeconómica e institucional presentaron MAE inferior a 0.08 (ej. Australia: 0.054, Uruguay: 0.057, Canadá: 0.064, España: 0.072).

### KPI 3: Comparación Predicho vs. Real (Bandas de Precisión)
- **Alta Precisión ($|\text{Error}| \le 0.25$):** 68.4% de los eventos procesados.
- **Aceptable ($0.25 < |\text{Error}| \le 0.50$):** 22.8% de los eventos procesados.
- **Divergente ($|\text{Error}| > 0.50$):** 8.8% de los eventos procesados.

### KPI 4: Tendencia Temporal (2015 - 2019)
- **2015:** 158 eventos | Score Real: 5.376 | Predicho: 5.350 | MAE: 0.2678
- **2016:** 157 eventos | Score Real: 5.382 | Predicho: 5.296 | MAE: 0.2444
- **2017:** 155 eventos | Score Real: 5.354 | Predicho: 5.414 | MAE: 0.2257
- **2018:** 156 eventos | Score Real: 5.376 | Predicho: 5.392 | MAE: 0.2223
- **2019:** 156 eventos | Score Real: 5.407 | Predicho: 5.446 | MAE: 0.2531

### 7.1. Integración Nativa con Microsoft Power BI
Para cumplir la sugerencia explícita del docente y la rúbrica sobre herramientas BI oficiales:
1. **Archivo de Origen de Datos (`dashboards/happiness_analytics.pbids`):** Permite abrir Power BI Desktop en 1 clic preconfigurado para consultar PostgreSQL en `localhost:6432/happiness_db`.
2. **Consultas M (`dashboards/power_bi_queries.m`):** Código de extracción para cargar `dim_country`, `dim_date`, `fact_predictions` y `raw_happiness_events`.
3. **Fórmulas DAX (`dashboards/power_bi_dax_measures.dax`):** Medidas analíticas formales (`Total_Predictions`, `MAE`, `RMSE`, `Prediction_Bias`, `Avg_Actual_Score`, `Avg_Predicted_Score` y la columna de clasificación `Precision_Bracket`).
4. **Guía de Montaje:** Explicada paso a paso en [`dashboards/POWER_BI_GUIDE.md`](dashboards/POWER_BI_GUIDE.md).

---

## 8. GUÍA DE DEMOSTRACIÓN EN VIVO (COMANDOS DE TERMINAL)

Para realizar una sustentación fluida y contundente frente al docente, ejecuta los siguientes comandos en orden:

### Paso 1: Levantar los contenedores y verificar salud
```bash
docker compose up -d
docker compose ps
python src/verify_db.py
```
*Explicar:* Se levantan Zookeeper, Kafka y PostgreSQL en Docker. `verify_db.py` comprueba que las 4 tablas del Star Schema y sus 3 foreign keys estén activas en PostgreSQL.

### Paso 2: Mostrar el Tópico en Kafka
```bash
docker exec happiness_kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic happiness-events
```
*Explicar:* Muestra que el tópico `happiness-events` tiene exactamente 3 particiones y factor de replicación 1.

### Paso 3: Lanzar Consumidor y Productor en Vivo
Abre dos pestañas de terminal:
- **Terminal 1 (Consumidor):**
  ```bash
  python kafka/consumer.py --timeout-ms 25000
  ```
- **Terminal 2 (Productor):**
  ```bash
  python kafka/producer.py --delay 0.005
  ```
*Explicar:* Observa en la terminal 1 cómo el consumidor procesa en tiempo real los eventos, realiza la inferencia con `model.pkl` e imprime advertencias cuando detecta los 4 eventos malformados sin tumbar el pipeline.

### Paso 4: Consultar los KPIs directamente en PostgreSQL
```bash
python src/run_kpis.py
```
*Explicar:* Ejecuta las 4 consultas analíticas en vivo sobre la base de datos PostgreSQL, demostrando que los datos están cargados en el Star Schema y que los resultados no provienen de archivos CSV.

---

## 9. BANCO DE PREGUNTAS DIFÍCILES DE SUSTENTACIÓN ORAL (30% DE LA NOTA)

Aquí tienes las respuestas técnicas fundamentadas a las preguntas más exigentes que el docente evaluador podría formularte:

### Pregunta 1: ¿Por qué configuramos 3 particiones en Kafka y por qué usamos `country` como la llave del mensaje?
> **Respuesta:** En Apache Kafka, las particiones son la unidad básica de paralelismo y escalabilidad. Tener 3 particiones permite que hasta 3 instancias del consumidor (dentro del grupo `happiness-ml-team`) lean en paralelo si se desea escalar horizontalmente. Al definir `country` como llave (`key`), el particionador de Kafka aplica un algoritmo de hash consistente ($hash(key) \pmod 3$). Esto garantiza dos propiedades críticas:
> 1. Todos los eventos de un mismo país van invariablemente a la **misma partición**, preservando el orden cronológico estricto de llegada por entidad.
> 2. Se distribuye la carga equitativamente entre las 3 particiones disponibles.

### Pregunta 2: ¿Qué es exactamente el *Target Leakage* en este problema y por qué fue imperativo descartar columnas como `Happiness Rank` y `Dystopia Residual`?
> **Respuesta:** El *target leakage* (fuga de la variable objetivo) ocurre cuando un modelo de Machine Learning es entrenado con variables que no estarán disponibles en el momento de realizar una inferencia en producción o que contienen directamente la respuesta matemática del target:
> 1. `Happiness Rank`: Es un atributo calculado ordenando de mayor a menor el `Happiness Score`. Si lo dejamos como feature, el modelo aprende una función monotónica inversa trivial. Además, cuando un país nuevo emite un evento en streaming, es imposible conocer su ranking de antemano.
> 2. `Dystopia Residual`: La fórmula metodológica del World Happiness Report define que $Score = \sum(\text{6 factores}) + \text{Dystopia Residual}$. Si incluimos el residuo, la regresión se degrada a una simple suma aritmética, perdiendo toda capacidad de generalización sobre datos reales futuros.

### Pregunta 3: ¿Por qué el consumidor almacena el mensaje en `raw_happiness_events` ANTES de validar y antes de inferir?
> **Respuesta:** Este es el principio fundamental de auditoría y desacoplamiento en Data Engineering (patrón *Landing / Staging*):
> 1. **Tolerancia a Fallos y Auditoría Inmutable:** Si un evento viene corrupto o mal formado, no debe perderse. Al guardarlo en crudo con un estado `INVALID_SCHEMA` o `INVALID_VALUES`, los ingenieros pueden auditar el payload exacto para diagnosticar fallas en el productor o en el contrato.
> 2. **Capacidad de Reprocesamiento (Replayability):** Si en el futuro se actualiza el modelo a una versión más avanzada (`model_v2.pkl`) o se flexibilizan las reglas de validación, los eventos crudos almacenados pueden ser reprocesados sin necesidad de retransmitirlos desde Kafka.

### Pregunta 4: ¿Por qué `raw_happiness_events` NO se modela como una dimensión en el Star Schema?
> **Respuesta:** Una dimensión en el modelado dimensional de Kimball describe el contexto de negocio de un evento (quién, cuándo, dónde) y típicamente presenta atributos conformados con cardinalidad acotada o controlada. `raw_happiness_events` es una tabla de *landing* / auditoría técnica que crece al mismo ritmo o incluso a mayor volumen que la tabla de hechos (almacena también los registros inválidos). Por ende, se modela como una tabla de auditoría externa que se vincula $1:1$ a la tabla de hechos mediante `raw_event_id` para garantizar la trazabilidad forense de cada predicción.

### Pregunta 5: ¿Por qué empaquetamos el preprocesamiento (`SimpleImputer` y `StandardScaler`) dentro de un `Pipeline` de scikit-learn en lugar de preprocesar el dataframe y entrenar el modelo por separado?
> **Respuesta:** Por dos razones arquitectónicas:
> 1. **Prevención de Data Leakage en Batch:** El escalador calcula $\mu$ y $\sigma$ exclusivamente sobre `X_train`. Si escalamos todo el dataset antes del split, la distribución del conjunto de prueba contamina el entrenamiento.
> 2. **Idempotencia y Simplicidad en Streaming:** En el consumidor de Kafka llega un único diccionario JSON. Al tener el `Pipeline` serializado en `model.pkl`, basta con invocar `pipeline.predict(df)`: el pipeline aplica de forma automática y reproducible la misma imputación y la misma escala aprendidas en el entrenamiento, evitando código duplicado y discrepancias de preprocesamiento entre offline y online (*train-serve skew*).

### Pregunta 6: ¿Cuál es la diferencia práctica entre MAE y RMSE en la evaluación de este modelo?
> **Respuesta:** El MAE (Mean Absolute Error) calcula el promedio de las diferencias absolutas entre lo real y lo predicho ($0.4077$ en nuestro test set), representando el error esperado en la misma escala del target. El RMSE (Root Mean Squared Error, $0.5239$), al elevar los residuos al cuadrado antes de promediar, penaliza con mucha mayor severidad los errores grandes o atípicos. La cercanía entre nuestro MAE (0.40) y RMSE (0.52) demuestra que el modelo tiene muy pocos errores extremos y mantiene una consistencia sólida a lo largo de todo el espectro de predicción.

### Pregunta 7: ¿Cómo se diseñó el Dashboard en Power BI Desktop y por qué se conecta a PostgreSQL en lugar de a los CSVs?
> **Respuesta:** En una arquitectura de Streaming ETL para producción, los eventos arriban en tiempo real a través de Apache Kafka y se cargan dinámicamente en PostgreSQL tras la inferencia con Scikit-learn. Si el dashboard leyera los CSVs planos, se trataría de un proceso batch estático desconectado del motor de streaming. Conectando Power BI directamente a PostgreSQL (`localhost:6432 / happiness_db`), el dashboard refleja el flujo continuo de inferencias respetando el Star Schema (`dim_country`, `dim_date`, `fact_predictions` y `raw_happiness_events`). Además, implementamos la plantilla oficial `Workshop3_Happiness_Dashboard.pbit` y el proyecto `Workshop3_Happiness.pbip` con los 4 KPIs ya preconfigurados visualmente (Tarjetas resumen, Columnas agrupadas de MAE/RMSE regional, Línea de tendencia temporal 2015-2019, Tabla de residuos por país y Anillo de resiliencia de ingesta).
