# Workshop-3: Streaming ETL con Apache Kafka y Machine Learning
**Programa Académico:** Ingeniería de Datos e Inteligencia Artificial  
**Curso:** ETL (G01) — Facultad de Ingeniería y Ciencias Básicas  
**Autor:** Mauricio Taborda (Taborda004)  
*Nota: Para la versión en inglés, consulte [README.md](README.md).*

---

## 1. Descripción General del Proyecto
Este proyecto implementa una arquitectura completa de **Streaming ETL orientada a eventos con inferencia de Machine Learning en tiempo real**. El sistema ingiere datasets históricos del Reporte Mundial de la Felicidad (2015–2019), armoniza sus esquemas heterogéneos, entrena un pipeline formal de regresión en `scikit-learn` libre de *target leakage*, transmite eventos vía **Apache Kafka** bajo un contrato estricto de mensajes JSON, ejecuta inferencia predictiva en milisegundos y persiste tanto la auditoría cruda como los resultados en un **Modelo Dimensional (Star Schema) sobre PostgreSQL 16**.

---

## 2. Arquitectura General del Sistema

El flujo técnico está dividido en dos subsistemas coordinados:

```
┌────────────────────────────────────────────────────────────────────────┐
│                     1. PIPELINE OFFLINE (BATCH ML)                     │
└────────────────────────────────────────────────────────────────────────┘
  Datasets Crudos (2015.csv a 2019.csv en data/raw/)
            │
            ▼
  Auditoría Exploratoria y Armonización (notebooks/eda.ipynb, src/harmonization.py)
            │
            ▼
  Dataset Maestro Unificado (data/processed/happiness_unified.csv)
            │
            ▼
  Partición Train/Test 70/30 (Sin Target Leakage, random_state=42)
            │
            ▼
  Pipeline Scikit-Learn (SimpleImputer + StandardScaler + RandomForestRegressor)
            │
            ▼
  Evaluación Rigurosa (MAE: 0.4077 | RMSE: 0.5239 | R²: 0.7802)
            │
            ▼
  Pipeline Serializado (models/model.pkl)
            │
            ▼
  Dataset de Origen de Streaming (data/streaming/happiness_stream_source.csv)

┌────────────────────────────────────────────────────────────────────────┐
│              2. STREAMING ETL EN TIEMPO REAL CON KAFKA                 │
└────────────────────────────────────────────────────────────────────────┘
  Dataset de Streaming (data/streaming/happiness_stream_source.csv)
            │
            ▼
  Kafka Producer (kafka/producer.py)
  - Emisión registro a registro con delay configurable
  - Llave del mensaje = 'country' (bytes UTF-8)
  - Tópico: happiness-events (3 particiones, factor de replicación 1)
  - Inyección de eventos malformados sintéticos (prueba de resiliencia)
            │
            ▼
  Broker Apache Kafka (Confluent Platform en Docker Compose)
            │
            ▼
  Kafka Consumer (kafka/consumer.py) — Grupo: happiness-ml-team
            │
            ├─► PASO 1: ATERRIZAJE CRUDO INMEDIATO (Auditoría Forense)
            │   Inserta el payload recibido en PostgreSQL 'raw_happiness_events'
            │   con estado inicial 'RECEIVED' antes de cualquier validación.
            │
            ├─► PASO 2: VALIDACIÓN DEL CONTRATO DE EVENTOS
            │   - Validación de campos requeridos (event_id, country, year, features...)
            │   - Validación de tipos numéricos y formato de marca temporal ISO-8601
            │   - Si es INVÁLIDO: actualiza a 'INVALID_SCHEMA' o 'INVALID_VALUES'
            │     con el motivo del error y continúa sin caerse jamás.
            │
            └─► PASO 3: INFERENCIA ML & CARGA AL STAR SCHEMA (Para eventos VALID)
                - Actualiza estado a 'VALID' en tabla cruda
                - Resuelve / Upsert en dimensiones 'dim_country' y 'dim_date'
                - Aplica orden exacto de variables y predice con 'models/model.pkl'
                - Calcula el residuo: prediction_error = actual_score - predicted_score
                - Inserta el hecho en 'fact_predictions' (con FKs)
            │
            ▼
  Base de Datos PostgreSQL 16 (Star Schema en localhost:6432/happiness_db)
            │
            ▼
  Capa de Analítica, Dashboards y KPIs (sql/kpis.sql & dashboards/)
  - KPI 1: Error Medio Absoluto (MAE) y RMSE global y por región
  - KPI 2: Volumen y desempeño de predicciones por país
  - KPI 3: Comparación Predicho vs Real y bandas de precisión
  - KPI 4: Tendencias temporales (2015-2019)
```

---

## 3. Decisiones de Limpieza y Armonización de Esquemas

### Diagnóstico de los Datasets Crudos
Al examinar los 5 archivos anuales (2015–2019) se evidenciaron variaciones críticas:
- **Evolución del número de columnas:** 12 columnas en 2015, 13 en 2016, 12 en 2017 y 9 en 2018–2019.
- **Inconsistencias en nombres de atributos:** e.g., `Happiness Score` (2015/16) frente a `Happiness.Score` (2017) y `Score` (2018/19); `Economy (GDP per Capita)` frente a `GDP per capita`; `Family` frente a `Social support`.
- **Ausencia de información geográfica:** El atributo `Region` solo estaba presente en 2015 y 2016.
- **Calidad de datos:** A lo largo de las 782 observaciones históricas existía exactamente **un único valor nulo** (`Perceptions of corruption` en el registro de `United Arab Emirates` del año 2018).

### Decisiones Técnicas Implementadas:
1. **Esquema Unificado (`snake_case`):** Estandarización a: `country`, `region`, `year`, `actual_happiness_score`, `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`.
2. **Tratamiento del Valor Nulo:** En lugar de eliminar la fila o imputar una media general, se empleó la **mediana histórica del propio país** (EAU reportó 0.3858 en 2015, 0.3556 en 2016, 0.3245 en 2017 y 0.1820 en 2019; mediana: `0.3400`), protegiendo la serie temporal.
3. **Enriquecimiento Regional:** Se creó un mapa de referencia cruzada con las regiones de 2015/2016 para propagar la región a los países de 2017–2019, garantizando cobertura total en `dim_country`.

---

## 4. Ingeniería de Características y Prevención de Target Leakage

### Eliminación Estricta de Fuga de Datos (*Target Leakage*):
Para acatar la rúbrica y las buenas prácticas de ingeniería de ML, se descartaron:
- `Happiness Rank` / `Overall rank`: Atributo derivado directamente del ordenamiento de la variable objetivo. En producción, un evento de streaming entrante no dispone del ranking previo.
- `Standard Error`, `Whisker.high`, `Whisker.low`: Intervalos de dispersión calculados directamente sobre las respuestas del Score.
- `Dystopia Residual`: La metodología del reporte suma los factores socioeconómicos más el residuo de distopía para dar el score. Incluir el residuo convierte el aprendizaje en una suma trivial e inutilizable en escenarios reales.

### Vector Final de Variables:
$$\mathbf{X} = [\text{gdp}, \text{family}, \text{health}, \text{freedom}, \text{generosity}, \text{corruption}]$$
$$\mathbf{y} = \text{actual\_happiness\_score}$$

### Pipeline Formal en `scikit-learn`:
- **`SimpleImputer(strategy='median')`:** Previene fallos ante posibles nulos en tiempo de inferencia streaming.
- **`StandardScaler()`:** Normaliza la escala de las 6 variables predictoras.
- **`RandomForestRegressor(n_estimators=100, max_depth=10, min_samples_split=4, random_state=42)`:** Modela interacciones socioeconómicas no lineales.

### Resultados en el Conjunto de Prueba (30% Test, 235 Registros):
- **MAE (Mean Absolute Error):** `0.4077` (Desviación promedio de solo 0.4 puntos en escala de 0 a 10).
- **RMSE (Root Mean Squared Error):** `0.5239`.
- **$R^2$ Score:** `0.7802` (Explica más del 78% de la varianza total).

---

## 5. Pipeline de Kafka y Especificación del Contrato de Eventos

### Configuración del Broker y Tópico
- **Nombre del Tópico:** `happiness-events`
- **Particiones:** 3 (balanceo de carga y paralelismo de consumo).
- **Factor de Replicación:** 1.
- **Grupo de Consumidores:** `happiness-ml-team`

### Contrato de Eventos (Formato JSON Estricto)
```json
{
  "event_id": "E0001",
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2,
  "event_time": "2026-11-08T20:30:00"
}
```

### Reglas de Validación:
1. `event_id`: Cadena no vacía; identificador único.
2. `country`: Cadena no vacía; actúa como la clave del mensaje en Kafka.
3. `year`: Entero entre 1900 y 2100.
4. `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`: Valores numéricos finitos y válidos.
5. `actual_happiness_score`: Valor numérico exclusivo para evaluación posterior (no entra al modelo).
6. `event_time`: Marca temporal en formato ISO-8601.

### Resiliencia y Tolerancia a Fallos:
- **Aterrizaje previo:** Todo mensaje recibido se guarda primero en `raw_happiness_events` con estado `RECEIVED`.
- **Manejo de Errores:** Si el evento presenta anomalías (esquema incompleto, valores corruptos o fechas inválidas), se actualiza a `INVALID_SCHEMA` o `INVALID_VALUES` registrando la justificación en `error_message`.
- **Continuidad Operativa:** El consumidor descarta la predicción para eventos inválidos y **nunca detiene su ejecución**.

---

## 6. Modelo Dimensional en PostgreSQL (Star Schema)

Implementado en `sql/create_tables.sql`:

```
┌────────────────────────────────────────────────────────┐
│                   raw_happiness_events                 │
├────────────────────────────────────────────────────────┤
│ raw_event_id (PK, BIGSERIAL)                           │
│ event_id (VARCHAR)                                     │
│ country (VARCHAR)                                      │
│ year (INT)                                             │
│ raw_payload (JSONB)                                    │
│ processing_status (VARCHAR)                            │
│ error_message (TEXT)                                   │
│ ingestion_timestamp (TIMESTAMPTZ)                      │
└──────────────────────────┬─────────────────────────────┘
                           │ 1:1
                           ▼
┌────────────────────────────────────────────────────────┐
│                     fact_predictions                   │
├────────────────────────────────────────────────────────┤
│ prediction_id (PK, BIGSERIAL)                          │
│ raw_event_id (FK -> raw_happiness_events)              │
│ country_id (FK -> dim_country)                         │
│ date_id (FK -> dim_date)                               │
│ gdp, family, health, freedom, generosity, corruption   │
│ actual_score (FLOAT)                                   │
│ predicted_score (FLOAT)                                │
│ prediction_error (FLOAT) [actual - predicted]          │
│ event_time (TIMESTAMP)                                 │
│ prediction_timestamp (TIMESTAMPTZ)                     │
└──────────────┬──────────────────────────┬──────────────┘
               │ N:1                      │ N:1
               ▼                          ▼
┌───────────────────────────┐  ┌─────────────────────────┐
│        dim_country        │  │         dim_date        │
├───────────────────────────┤  ├─────────────────────────┤
│ country_id (PK, SERIAL)   │  │ date_id (PK, SERIAL)    │
│ country_name (VARCHAR, UQ)│  │ year (INT)              │
│ region (VARCHAR)          │  │ event_time (TIMESTAMP)  │
│ created_at (TIMESTAMPTZ)  │  │ month, day, hour, min   │
└───────────────────────────┘  └─────────────────────────┘
```

---

## 7. KPIs Analíticos y Resultados Oficiales

Consultas analíticas implementadas en `sql/kpis.sql` ejecutadas sobre PostgreSQL:

### KPI 1: Error Promedio de Predicción
- **MAE Global:** `0.2427` puntos.
- **RMSE Global:** `0.3434` puntos.
- **Sesgo Medio:** `-0.0002` (modelo no sesgado).
- **Desempeño Regional:**
  - *Menor error:* Oceanía (`0.0786`), Norteamérica (`0.0880`), Europa Occidental (`0.1997`).
  - *Mayor dispersión:* América Latina (`0.3128`), Asia del Sur (`0.3229`).

### KPI 2: Predicciones por País
Se generaron predicciones longitudinales para más de 150 naciones, con MAE inferior a 0.08 en países institucionalmente consolidados (Australia: 0.054, Uruguay: 0.057, Canadá: 0.064).

### KPI 3: Predicho vs Real (Bandas de Precisión)
- **Alta Precisión ($|\text{Error}| \le 0.25$):** 68.4% de los registros procesados.
- **Aceptable ($0.25 < |\text{Error}| \le 0.50$):** 22.8%.
- **Divergente ($|\text{Error}| > 0.50$):** 8.8%.

### KPI 4: Tendencias Temporales (2015–2019)
- **2015:** 158 eventos | Score Real: 5.376 | Predicho: 5.350 | MAE: 0.2678
- **2016:** 157 eventos | Score Real: 5.382 | Predicho: 5.296 | MAE: 0.2444
- **2017:** 155 eventos | Score Real: 5.354 | Predicho: 5.414 | MAE: 0.2257
- **2018:** 156 eventos | Score Real: 5.376 | Predicho: 5.392 | MAE: 0.2223
- **2019:** 156 eventos | Score Real: 5.407 | Predicho: 5.446 | MAE: 0.2531

### Auditoría de Ingesta y Resiliencia
- Total Eventos Recibidos: **786**
- Eventos Válidos e Inferidos: **782** (99.49%)
- Eventos Inválidos Filtrados: **4** (0.51%)
- **Caídas del Pipeline:** 0

---

## 8. Estructura Completa del Repositorio

```
workshop_3/
├── data/
│   ├── raw/                 # Datasets originales (2015.csv - 2019.csv)
│   ├── processed/           # happiness_unified.csv
│   └── streaming/           # happiness_stream_source.csv
├── notebooks/
│   ├── eda.ipynb            # Paso 1 & 2: EDA, limpieza y esquema unificado
│   └── model_training.ipynb # Paso 3 & 4: Modelado, métricas y serialización
├── src/
│   ├── harmonization.py     # Módulo de armonización y preprocesamiento
│   ├── train_model.py       # Script de entrenamiento y verificación unitaria
│   ├── run_kpis.py          # Ejecución de los 4 KPIs en PostgreSQL
│   ├── generate_dashboard_charts.py # Generador de figuras de alta resolución
│   └── verify_db.py         # Validador de tablas, tipos y Foreign Keys
├── kafka/
│   ├── producer.py          # Productor de eventos con delay y prueba de fallos
│   └── consumer.py          # Consumidor con inferencia en vivo y tolerancia a fallos
├── models/
│   └── model.pkl            # Pipeline Scikit-Learn serializado
├── sql/
│   ├── create_tables.sql    # DDL del Star Schema en PostgreSQL
│   └── kpis.sql             # 4 Consultas SQL analíticas para los KPIs
├── dashboards/
│   ├── happiness_analytics.pbids # Conexión directa en 1 clic a Power BI Desktop
│   ├── POWER_BI_GUIDE.md    # Guía detallada para construcción del informe en Power BI
│   ├── power_bi_dax_measures.dax # Fórmulas DAX para los 4 KPIs
│   ├── power_bi_queries.m   # Consultas Power Query M para PostgreSQL
│   ├── dashboard.html       # Dashboard interactivo moderno (abrir en navegador)
│   ├── kpis_report.md       # Reporte detallado de los KPIs
│   └── *.png, *.csv         # Gráficas de alta resolución y tablas exportadas
├── docker-compose.yml       # Orquestación de Kafka, Zookeeper y PostgreSQL
├── requirements.txt         # Dependencias exactas del proyecto
├── README.md                # Documentación oficial en inglés
├── README_ES.md             # Esta documentación técnica en español
└── GUIA_EXPLICACION_PROYECTO.md # Guía exhaustiva de estudio para la sustentación oral (30%)
```

---

## 9. Guía de Ejecución Rápida Paso a Paso

### 1. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 2. Iniciar Infraestructura en Docker
```bash
docker compose up -d
```
*Kafka se expone en `localhost:9092` y PostgreSQL en `localhost:6432` (`happiness_db`).*

### 3. Verificar Tablas en PostgreSQL
```bash
python src/verify_db.py
```

### 4. Armonización de Datos y Entrenamiento de ML
```bash
python src/harmonization.py
python src/train_model.py
```

### 5. Iniciar Streaming en Tiempo Real
En Terminal 1:
```bash
python kafka/consumer.py --timeout-ms 25000
```

En Terminal 2:
```bash
python kafka/producer.py --delay 0.005
```

### 6. Ejecutar KPIs y Ver el Dashboard
```bash
python src/run_kpis.py
python src/generate_dashboard_charts.py
```
Abre el archivo `dashboards/dashboard.html` en tu navegador web para visualizar el panel interactivo completo.
