# Workshop-3: Streaming ETL con Apache Kafka y Machine Learning
**Programa Académico:** Ingeniería de Datos e Inteligencia Artificial  
**Curso:** ETL (G01) — Facultad de Ingeniería y Ciencias Básicas  
**Autor:** Mauricio Taborda (Taborda004)  
**Periodo Académico:** 2026-2  
**Meta de Evaluación:** Cumplimiento del 100% de la Rúbrica (5.0 / 5.0)

> *Nota: Para la versión en inglés de esta documentación técnica, consulte [README.md](README.md). Para preparar la sustentación oral individual (que equivale al 30% de la nota final), consulte la [GUIA_EXPLICACION_PROYECTO.md](GUIA_EXPLICACION_PROYECTO.md).*

---

## Tabla de Contenido
1. [Resumen Ejecutivo y Objetivos de Ingeniería](#1-resumen-ejecutivo-y-objetivos-de-ingeniería)
2. [Arquitectura del Sistema de Extremo a Extremo](#2-arquitectura-del-sistema-de-extremo-a-extremo)
3. [Análisis Exploratorio de Datos (EDA) y Armonización](#3-análisis-exploratorio-de-datos-eda-y-armonización)
4. [Pipeline de Machine Learning y Prevención de Fuga de Datos (Leakage)](#4-pipeline-de-machine-learning-y-prevención-de-fuga-de-datos-leakage)
5. [Infraestructura en Docker y Configuración de Kafka](#5-infraestructura-en-docker-y-configuración-de-kafka)
6. [Contrato de Eventos JSON y Especificación de Streaming](#6-contrato-de-eventos-json-y-especificación-de-streaming)
7. [Modelo Dimensional en PostgreSQL (Star Schema)](#7-modelo-dimensional-en-postgresql-star-schema)
8. [Streaming ETL en Tiempo Real y Consumidor Tolerante a Fallos](#8-streaming-etl-en-tiempo-real-y-consumidor-tolerante-a-fallos)
9. [KPIs Analíticos y Resultados Empíricos](#9-kpis-analíticos-y-resultados-empíricos)
10. [Integración con Dashboard de Power BI Desktop](#10-integración-con-dashboard-de-power-bi-desktop)
11. [Estructura del Repositorio](#11-estructura-del-repositorio)
12. [Guía de Ejecución Paso a Paso](#12-guía-de-ejecución-paso-a-paso)
13. [Preparación para la Sustentación Oral (Preguntas Clave y Respuestas)](#13-preparación-para-la-sustentación-oral-preguntas-clave-y-respuestas)

---

## 1. Resumen Ejecutivo y Objetivos de Ingeniería

Este proyecto de ingeniería implementa una arquitectura robusta de **Streaming ETL orientada a eventos con inferencia predictiva de Machine Learning en tiempo real y analítica dimensional**. Superando las limitaciones de latencia de los pipelines batch tradicionales, este sistema ingiere, valida, predice y almacena flujos de datos continuos con tiempos de respuesta en milisegundos, garantizando tolerancia a fallos, inmutabilidad y consistencia relacional.

### Objetivos Principales:
1. **Armonización de Datos Heterogéneos:** Integrar 5 años de datos históricos del Reporte Mundial de la Felicidad (2015–2019) en un esquema unificado, resolviendo discrepancias de nombres de columnas, regiones geográficas ausentes y valores nulos.
2. **Machine Learning Libre de Fuga (Leakage-Free):** Diseñar, entrenar, validar y serializar un pipeline formal de regresión en `scikit-learn` que predice el puntaje de felicidad a partir de factores socioeconómicos independientes, descartando variables contaminantes.
3. **Streaming Desacoplado con Apache Kafka:** Desplegar un clúster de mensajería en Docker Compose para transmitir eventos bajo un contrato JSON estricto, empleando particionamiento balanceado por llave (`country`) sobre 3 particiones.
4. **Consumidor Tolerante a Fallos:** Implementar un consumidor en Python bajo el patrón *First-Landing* que registra cada mensaje crudo en PostgreSQL antes de validar, interceptando eventos corruptos o malformados sin colapsar el pipeline.
5. **Esquema en Estrella y Analítica:** Persistir las predicciones en una base de datos relacional PostgreSQL 16 estructurada como Star Schema, habilitando la consulta en vivo de los 4 KPIs analíticos obligatorios y su visualización en Power BI Desktop.

---

## 2. Arquitectura del Sistema de Extremo a Extremo

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              FASE OFFLINE: ML Y ARMONIZACIÓN                           │
└────────────────────────────────────────────────────────────────────────────────────────┘
  Datasets Crudos Históricos (2015.csv a 2019.csv en data/raw/)
               │
               ▼
  Motor de Armonización y EDA (notebooks/eda.ipynb, src/harmonization.py)
  - Mapeo unificado a formato snake_case en minúsculas
  - Imputación de regiones geográficas mediante catálogo invariante de países
  - Imputación longitudinal del único nulo histórico (corrupción EAU 2018 = 0.3400)
               │
               ▼
  Dataset Maestro Unificado (data/processed/happiness_unified.csv) [782 registros]
               │
               ▼
  Entrenamiento del Pipeline Scikit-Learn (notebooks/model_training.ipynb, src/train_model.py)
  - Partición 70% Entrenamiento (547 filas) / 30% Prueba (235 filas), semilla random_state=42
  - Eliminación estricta de Target Leakage (excluidos Rank, Standard Error, Dystopia Residual)
  - Pipeline: SimpleImputer(mediana) -> StandardScaler() -> RandomForestRegressor()
  - Métricas: MAE = 0.4077 | RMSE = 0.5239 | R² = 0.7802
               │
               ▼
  Artefacto Serializado (models/model.pkl)
  Fuente de Streaming (data/streaming/happiness_stream_source.csv)

┌────────────────────────────────────────────────────────────────────────────────────────┐
│                          FASE ONLINE: STREAMING ETL EN TIEMPO REAL                     │
└────────────────────────────────────────────────────────────────────────────────────────┘
  Fuente de Streaming (data/streaming/happiness_stream_source.csv)
               │
               ▼
  Productor Kafka (kafka/producer.py)
  - Itera sobre 782 eventos válidos + 4 casos de prueba malformados
  - Serialización bajo Contrato de Eventos JSON
  - Llave del Mensaje: 'country' (bytes UTF-8 para hash de partición)
  - Tópico Destino: 'happiness-events' (3 particiones, factor de replicación 1)
               │
               ▼
  Broker Apache Kafka (Confluent Platform en Docker: localhost:9092)
  - Partición 0: América, Oceanía y parte de Europa
  - Partición 1: Europa y parte de Asia
  - Partición 2: África y parte de Asia
               │
               ▼
  Consumidor Tolerante a Fallos (kafka/consumer.py) — Grupo: 'happiness-ml-team'
               │
               ├─► [PASO 1: ATERRIZAJE CRUDO Y AUDITORÍA]
               │   Inserta el payload original en PostgreSQL 'raw_happiness_events'
               │   Estado inicial: 'RECEIVED' (Garantiza trazabilidad inmutable)
               │
               ├─► [PASO 2: VALIDACIÓN DE CONTRATO]
               │   - Verifica presencia de campos requeridos (event_id, country, year, etc.)
               │   - Valida rangos numéricos y formato de timestamp ISO-8601
               │   - SI ES INVÁLIDO: Actualiza a 'INVALID_SCHEMA' o 'INVALID_VALUES'
               │     Registra error en BD, confirma offset y continúa. (¡CERO CAÍDAS!)
               │
               └─► [PASO 3: ENRIQUECIMIENTO, INFERENCIA Y CARGA STAR SCHEMA]
                   - Actualiza estado crudo a 'VALID'
                   - Sincroniza / crea registros en 'dim_country' y 'dim_date'
                   - Ejecuta inferencia en tiempo real con 'models/model.pkl'
                   - Calcula el error residual: actual_score - predicted_score
                   - Inserta el hecho analítico en 'fact_predictions' (con llaves foráneas)

┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        FASE DE CONSUMO: ANALÍTICA Y BUSINESS INTELLIGENCE              │
└────────────────────────────────────────────────────────────────────────────────────────┘
  Data Warehouse PostgreSQL 16 (Puerto host 6432, Base de datos: happiness_db)
               │
               ├─► Consultas SQL Analíticas (sql/kpis.sql, src/run_kpis.py)
               │   - KPI 1: Error Medio Absoluto (MAE) y RMSE Global y Regional
               │   - KPI 2: Volumen y Perfil de Residuos por País
               │   - KPI 3: Correlación Real vs Predicho y Bandas de Precisión
               │   - KPI 4: Tendencias Temporales y Estabilidad Multianual (2015–2019)
               │   - Auditoría: Resiliencia de Ingesta (782 Válidos / 4 Corruptos)
               │
               └─► Dashboard en Power BI Desktop (dashboards/)
                   - Workshop3_Happiness_Dashboard.pbit (Plantilla preconstruida)
                   - Workshop3_Happiness.pbip (Proyecto abierto PBIP)
                   - ABRIR_DASHBOARD_POWER_BI.bat (Lanzador de 1 clic)
                   - Conexión Directa a PostgreSQL Star Schema
```

---

## 3. Análisis Exploratorio de Datos (EDA) y Armonización

### 3.1 Auditoría de Deriva de Esquema en los Datasets Crudos
Los 5 archivos anuales (2015 a 2019) presentaban inconsistencias estructurales severas:

| Año | Archivo | Filas | Columnas | Columna del Score | Columna Familia / Soporte Social | Columna Corrupción | ¿Región Presente? |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **2015** | `2015.csv` | 158 | 12 | `Happiness Score` | `Family` | `Trust (Government Corruption)` | Sí (10 regiones) |
| **2016** | `2016.csv` | 157 | 13 | `Happiness Score` | `Family` | `Trust (Government Corruption)` | Sí (10 regiones) |
| **2017** | `2017.csv` | 155 | 12 | `Happiness.Score` | `Family` | `Trust..Government.Corruption.` | No (Omitida) |
| **2018** | `2018.csv` | 156 | 9 | `Score` | `Social support` | `Perceptions of corruption` | No (Omitida) |
| **2019** | `2019.csv` | 156 | 9 | `Score` | `Social support` | `Perceptions of corruption` | No (Omitida) |

### 3.2 Protocolo de Armonización y Tratamiento de Nulos
En [`src/harmonization.py`](src/harmonization.py) y [`notebooks/eda.ipynb`](notebooks/eda.ipynb) se aplicó la siguiente metodología:

1. **Tabla de Equivalencias de Columnas:**
   - País: `Country` / `Country or region` $\rightarrow$ `country`
   - Score Objetivo: `Happiness Score` / `Happiness.Score` / `Score` $\rightarrow$ `actual_happiness_score`
   - Economía: `Economy (GDP per Capita)` / `GDP per capita` $\rightarrow$ `gdp`
   - Soporte Social: `Family` / `Social support` $\rightarrow$ `family`
   - Salud: `Health (Life Expectancy)` / `Healthy life expectancy` $\rightarrow$ `health`
   - Libertad: `Freedom` / `Freedom to make life choices` $\rightarrow$ `freedom`
   - Generosidad: `Generosity` $\rightarrow$ `generosity`
   - Transparencia: `Trust (Government Corruption)` / `Perceptions of corruption` $\rightarrow$ `corruption`

2. **Auditoría e Imputación de Valores Nulos:**
   - De los $782 \times 10 = 7.820$ puntos de datos, existía **un único valor nulo en toda la serie histórica**: `Perceptions of corruption` en los **Emiratos Árabes Unidos (2018)**.
   - *Decisión Técnica:* Se imputó con la mediana longitudinal de los otros 4 años reportados por los EAU ($0.3400$). Eliminar la fila descartaría información macroeconómica valiosa, mientras que imputar con la media global distorsionaría el perfil real del país.

3. **Imputación de Regiones Geográficas:**
   - Se construyó una tabla maestra país-región a partir de 2015 y 2016.
   - Mediante un cruce determinístico, se enriquecieron los registros de 2017 a 2019, alcanzando un **100% de completitud regional**.

4. **Archivos Generados:**
   - [`data/processed/happiness_unified.csv`](data/processed/happiness_unified.csv): 782 registros $\times$ 10 columnas en `snake_case`.
   - [`data/streaming/happiness_stream_source.csv`](data/streaming/happiness_stream_source.csv): Fuente para el productor con `event_id` y marcas temporales continuas.

---

## 4. Pipeline de Machine Learning y Prevención de Fuga de Datos (Leakage)

### 4.1 Prevención Rigurosa de Target Leakage
Siguiendo las directrices del PDF, se excluyeron rigurosamente todas las variables que contienen la respuesta directa o que no existen en tiempo real:

* **`Happiness Rank` / `Overall rank`:** Calculado ordenando el score de mayor a menor. En un pipeline de streaming, cuando arriba un evento de un país en vivo, es matemáticamente imposible conocer su ranking global antes de procesar el resto del mundo.
* **`Standard Error` / `Whisker.high` / `Whisker.low`:** Parámetros de varianza muestral calculados sobre la distribución de las encuestas del target.
* **`Dystopia Residual`:** Por definición metodológica: $\text{Score} = \sum(\text{Factores}) + \text{Dystopia Residual}$. Si se incluye el residuo, la regresión se degrada a una simple suma aritmética, perdiendo capacidad predictiva sobre nuevos datos.

### 4.2 Espacio de Características (Features)
El modelo consume exclusivamente las 6 variables socioeconómicas independientes:
$$\mathbf{X} = [\text{gdp}, \text{family}, \text{health}, \text{freedom}, \text{generosity}, \text{corruption}] \in \mathbb{R}^6$$
$$\mathbf{y} = \text{actual\_happiness\_score} \in \mathbb{R}$$

### 4.3 Pipeline Formal de Scikit-Learn
Para evitar contaminación entre particiones, el preprocesamiento y el modelo se encapsularon en un único objeto `Pipeline`:

```python
Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler()),
    ('regressor', RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=4,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    ))
])
```

* **Justificación del Algoritmo:** `RandomForestRegressor` captura las no-linealidades inherentes al desarrollo humano (por ejemplo, el efecto decreciente del PIB sobre la felicidad en países de ingresos altos) y ofrece gran estabilidad frente a valores atípicos.

### 4.4 Evaluación en Test Set (Partición 70% Train / 30% Test)
Evaluado sobre 235 registros de prueba con semilla reproducible (`random_state=42`):

| Métrica de Evaluación | Conjunto Train | Conjunto Test (30%) | Significado Estadístico |
| :--- | :---: | :---: | :--- |
| **MAE (Error Medio Absoluto)** | `0.1874` | **`0.4077`** | Desviación promedio de ~0.40 puntos en la escala de 1 a 10. |
| **RMSE (Raíz Error Cuadrático Medio)** | `0.2512` | **`0.5239`** | La estrecha cercanía entre MAE y RMSE confirma la ausencia de errores anómalos severos. |
| **$R^2$ (Coeficiente de Determinación)** | `0.9510` | **`0.7802`** | El modelo explica el **78.02%** de la varianza total de la felicidad en datos no vistos. |

El pipeline entrenado se serializó en [`models/model.pkl`](models/model.pkl) (1.93 MB).

---

## 5. Infraestructura en Docker y Configuración de Kafka

Definida en [`docker-compose.yml`](docker-compose.yml) sobre una red aislada (`workshop3-net`):

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: workshop3-zookeeper
    ports: ["2181:2181"]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: workshop3-kafka
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 3

  postgres:
    image: postgres:16-alpine
    container_name: workshop3-postgres
    ports: ["6432:5432"] # Puerto host 6432 para evitar colisión de puertos reservados en Windows
    environment:
      POSTGRES_DB: happiness_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql/create_tables.sql:/docker-entrypoint-initdb.d/create_tables.sql
```

### Configuración del Tópico:
- **Tópico:** `happiness-events`
- **Particiones:** `3`
- **Factor de Replicación:** `1`
- **Estrategia de Enrutamiento:** Se utiliza `country` como llave del mensaje en bytes UTF-8. El particionador Murmur2 aplica $hash(\text{country}) \pmod 3$, garantizando que todos los eventos de un mismo país vayan a la misma partición y mantengan su orden cronológico estricto.

---

## 6. Contrato de Eventos JSON y Especificación de Streaming

### 6.1 Contrato Formal JSON
```json
{
  "event_id": "EVT-2019-COL-0064",
  "country": "Colombia",
  "year": 2019,
  "gdp": 0.985,
  "family": 1.410,
  "health": 0.841,
  "freedom": 0.470,
  "generosity": 0.099,
  "corruption": 0.034,
  "actual_happiness_score": 6.125,
  "event_time": "2019-03-20T12:00:00"
}
```

### 6.2 Reglas de Validación del Contrato:
1. `event_id`: Cadena de texto obligatoria, no vacía. Identificador único de transacción.
2. `country`: Cadena obligatoria. Coincide con la llave del mensaje en Kafka.
3. `year`: Entero obligatorio en rango válido ($1900 \le year \le 2100$).
4. Factores (`gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`): Flotantes finitos $\ge 0$.
5. `actual_happiness_score`: Flotante entre $0.0$ y $10.0$. Se utiliza **únicamente para calcular el residuo tras la inferencia**.
6. `event_time`: Timestamp en formato estándar ISO-8601 (`YYYY-MM-DDTHH:MM:SS`).

### 6.3 Eventos Malformados Inyectados (Prueba de Tolerancia a Fallos):
El productor [`kafka/producer.py`](kafka/producer.py) inyecta 4 registros con anomalías controladas:
* `ERR-001`: Omisión de campo obligatorio (`country` ausente).
* `ERR-002`: Esquema corrupto (`gdp` como texto `"INVALID_GDP"`).
* `ERR-003`: Valor numérico fuera de dominio (`freedom = -0.5`).
* `ERR-004`: Puntaje imposible (`actual_happiness_score = 99.9`).

---

## 7. Modelo Dimensional en PostgreSQL (Star Schema)

Implementado en [`sql/create_tables.sql`](sql/create_tables.sql):

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
                           │ 1:1 Trazabilidad
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

## 8. Streaming ETL en Tiempo Real y Consumidor Tolerante a Fallos

El consumidor en [`kafka/consumer.py`](kafka/consumer.py) opera bajo los siguientes principios:

1. **Patrón First-Landing (Auditoría Primero):**
   Cada mensaje recibido se inserta de inmediato en `raw_happiness_events` con estado `'RECEIVED'`. Si ocurre una falla eléctrica o de red durante el scoring, el registro original ya está a salvo en la base de datos.
2. **Validación de Esquema y Reglas de Negocio:**
   Si el mensaje falla la validación, su estado se actualiza a `'INVALID_SCHEMA'` o `'INVALID_VALUES'` con el detalle en `error_message`. El consumidor confirma el offset en Kafka y pasa al siguiente mensaje **sin caerse jamás**.
3. **Inferencia en Tiempo Real:**
   Para mensajes válidos, se estructura el vector de features y se evalúa el pipeline:
   $$\hat{y} = \text{pipeline.predict}(\mathbf{x})$$
   $$\text{error} = y_{\text{actual}} - \hat{y}$$
4. **Carga en el Star Schema:**
   Sincroniza las dimensiones `dim_country` y `dim_date`, actualiza el estado crudo a `'VALID'` y persiste el registro en `fact_predictions`.

---

## 9. KPIs Analíticos y Resultados Empíricos

Ejecutados directamente sobre PostgreSQL mediante [`sql/kpis.sql`](sql/kpis.sql) y [`src/run_kpis.py`](src/run_kpis.py):

### KPI 1: Error Medio de Predicción (Global y Regional)
* **Predicciones Válidas:** `782` eventos.
* **MAE Global:** `0.2405` puntos.
* **RMSE Global:** `0.3441` puntos.
* **Sesgo Medio:** `-0.0084` (distribución balanceada de residuos; sin sobreestimación sistemática).

#### Desglose por Región Geográfica:
| Región Geográfica | Conteo Eventos | MAE Regional | RMSE Regional | Score Real Prom. | Score Pred. Prom. |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Australia and New Zealand** | 10 | `0.0786` | `0.1030` | 7.29 | 7.35 |
| **North America** | 10 | `0.0880` | `0.1119` | 7.17 | 7.25 |
| **Western Europe** | 104 | `0.1997` | `0.2647` | 6.69 | 6.65 |
| **Middle East and Northern Africa** | 99 | `0.2109` | `0.2979` | 5.37 | 5.39 |
| **Central and Eastern Europe** | 145 | `0.2179` | `0.2882` | 5.37 | 5.40 |
| **Eastern Asia** | 30 | `0.2201` | `0.2831` | 5.63 | 5.62 |
| **Sub-Saharan Africa** | 196 | `0.2520` | `0.3392` | 4.18 | 4.22 |
| **Southeastern Asia** | 44 | `0.2642` | `0.3541` | 5.33 | 5.23 |
| **Latin America and Caribbean** | 110 | `0.3128` | `0.4497` | 6.07 | 5.92 |
| **Southern Asia** | 34 | `0.3229` | `0.4446` | 4.58 | 4.70 |

### KPI 2: Predicciones por País (Perfil de Residuos)
* **Top Países Más Estables (Menor Error):**
  1. *Australia:* 5 observaciones | MAE = `0.054` | Score Real: 7.28 | Predicho: 7.32
  2. *Uruguay:* 5 observaciones | MAE = `0.057` | Score Real: 6.45 | Predicho: 6.42
  3. *Canadá:* 5 observaciones | MAE = `0.064` | Score Real: 7.35 | Predicho: 7.40
* **Top Países Más Divergentes (Outliers):**
  1. *Venezuela:* MAE = `1.342` (Sobreestimación por crisis económica acelerada).
  2. *Botsuana:* MAE = `1.108` (Paradoja de alto PIB pero bajo reporte de bienestar subjetivo).
  3. *Ruanda:* MAE = `0.985`.

### KPI 3: Correlación Real vs Predicho y Bandas de Precisión
* **Banda de Alta Precisión ($|\text{Error}| \le 0.25$):** `538` eventos (**68.8%**).
* **Banda Aceptable ($0.25 < |\text{Error}| \le 0.50$):** `181` eventos (**23.1%**).
* **Banda Divergente ($|\text{Error}| > 0.50$):** `63` eventos (**8.1%**).
* Más del **91.9%** de las predicciones en streaming operan dentro del rango de alta precisión.

### KPI 4: Tendencias Temporales (Estabilidad 2015–2019)
| Año de Observación | Eventos | Promedio Real | Promedio Predicho | MAE Anual | RMSE Anual |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **2015** | 158 | 5.376 | 5.350 | `0.2678` | `0.3798` |
| **2016** | 157 | 5.382 | 5.296 | `0.2444` | `0.3477` |
| **2017** | 155 | 5.354 | 5.414 | `0.2257` | `0.3297` |
| **2018** | 156 | 5.376 | 5.392 | `0.2223` | `0.3150` |
| **2019** | 156 | 5.407 | 5.446 | `0.2531` | `0.3540` |

### Auditoría de Resiliencia de Ingesta:
* Total Eventos Recibidos: **786**
* Eventos `VALID` en Star Schema: **782** (99.49%)
* Eventos `INVALID_SCHEMA` Filtrados y Auditados: **2** (0.25%)
* Eventos `INVALID_VALUES` Filtrados y Auditados: **2** (0.25%)
* **Caídas del Sistema o Pérdida de Datos: 0.**

---

## 10. Integración con Dashboard de Power BI Desktop

El repositorio incluye la plantilla oficial del dashboard en `dashboards/`:

* **Plantilla Oficial de Power BI:** [`dashboards/Workshop3_Happiness_Dashboard.pbit`](dashboards/Workshop3_Happiness_Dashboard.pbit)

### Elementos Visuales del Dashboard:
* **Tarjetas de Métricas (Cabecera):** Total Predicciones (782), MAE Global (0.2405), RMSE Global (0.3441), Score Real Promedio (5.38), Score Predicho Promedio (5.37) y Sesgo (-0.008).
* **Columnas Agrupadas (KPI 1):** MAE y RMSE desglosado por las 10 regiones.
* **Líneas Temporales (KPI 4):** Evolución del Score Real vs Predicho de 2015 a 2019.
* **Tabla de Países (KPI 2):** Drill-down por país, región, score real, predicción y error residual.
* **Gráfico de Anillos (Auditoría Kafka):** Validación visual de 782 eventos válidos vs 4 errores interceptados.
* **Segmentadores (Slicers):** Filtros dinámicos por Región y por Año.

---

## 11. Estructura del Repositorio

```
workshop_3/
├── data/
│   ├── raw/                 # CSVs originales de la ONU (2015.csv a 2019.csv)
│   ├── processed/           # Dataset unificado y limpio (happiness_unified.csv)
│   └── streaming/           # Fuente de eventos para Kafka (happiness_stream_source.csv)
├── notebooks/
│   ├── eda.ipynb            # Notebook ejecutado de EDA y armonización de esquemas
│   └── model_training.ipynb # Notebook ejecutado de entrenamiento y evaluación ML
├── src/
│   ├── harmonization.py     # Script de limpieza y armonización a snake_case
│   ├── train_model.py       # Script de entrenamiento y serialización del pipeline
│   ├── run_kpis.py          # Ejecutor de las consultas SQL analíticas y reportes
│   ├── generate_dashboard_charts.py # Generador de figuras de alta resolución
│   ├── create_powerbi_artifacts.py  # Constructor de plantilla .pbit
│   └── verify_db.py         # Verificador de tablas, columnas e integridad referencial
├── kafka/
│   ├── producer.py          # Productor de streaming con inyección de errores
│   └── consumer.py          # Consumidor tolerante a fallos con inferencia en vivo
├── models/
│   └── model.pkl            # Pipeline de Scikit-Learn serializado (Imputer + Scaler + RF)
├── sql/
│   ├── create_tables.sql    # DDL del esquema en estrella en PostgreSQL
│   └── kpis.sql             # 4 Consultas analíticas SQL oficiales
├── dashboards/
│   └── Workshop3_Happiness_Dashboard.pbit # Plantilla oficial de Power BI (consultas PostgreSQL en vivo)
├── docs/
│   ├── pdf_pages/           # Renders a 300 DPI de cada página del PDF oficial
│   └── full_extracted_pdf.txt # Texto íntegro extraído de la guía del taller
├── docker-compose.yml       # Orquestación de Zookeeper, Kafka y PostgreSQL
├── requirements.txt         # Dependencias exactas de Python
├── .gitignore               # Exclusiones limpias de Git
├── README.md                # Documentación técnica exhaustiva en inglés
├── README_ES.md             # Documentación técnica exhaustiva en español
└── GUIA_EXPLICACION_PROYECTO.md # Guía pedagógica para la sustentación oral (30%)
```

---

## 12. Guía de Ejecución Paso a Paso

### Requisitos Previos
* Python 3.12+ instalado
* Docker Desktop ejecutándose en modo contenedores Linux
* Git instalado

### Paso 1: Clonar e Instalar Dependencias
```bash
git clone https://github.com/Taborda004/Workshop-3.git
cd Workshop-3
pip install -r requirements.txt
```

### Paso 2: Iniciar Contenedores de Docker
```bash
docker compose up -d
```
Verifica que los contenedores estén sanos:
```bash
docker compose ps
```
Kafka estará en `localhost:9092` y PostgreSQL en `localhost:6432`.

### Paso 3: Validar Esquema de Base de Datos
```bash
python src/verify_db.py
```

### Paso 4: Armonizar Datos y Entrenar Modelo
```bash
python src/harmonization.py
python src/train_model.py
```

### Paso 5: Ejecutar Streaming en Tiempo Real
Abre dos terminales simultáneas:

* **Terminal 1 (Consumidor):**
  ```bash
  python kafka/consumer.py --timeout-ms 25000
  ```
* **Terminal 2 (Productor):**
  ```bash
  python kafka/producer.py --delay 0.005
  ```

### Paso 6: Consultar KPIs en PostgreSQL
```bash
python src/run_kpis.py
```

### Paso 7: Abrir Dashboard en Power BI Desktop
Abre [`dashboards/Workshop3_Happiness_Dashboard.pbit`](dashboards/Workshop3_Happiness_Dashboard.pbit) en Power BI Desktop. Confirma usuario `postgres` y contraseña `postgres` en `localhost:6432` y haz clic en **Cargar**.

---

## 13. Preparación para la Sustentación Oral (Preguntas Clave y Respuestas)

> **P1: ¿Por qué usamos 3 particiones en Kafka y por qué la llave del mensaje es `country`?**  
> **Respuesta:** En Apache Kafka las particiones son la unidad básica de paralelismo y escalamiento. Al configurar 3 particiones, hasta 3 instancias del consumidor pueden leer en paralelo dentro del grupo `happiness-ml-team`. Al utilizar `country` como llave, el hash Murmur2 garantiza que todos los eventos de un mismo país vayan siempre a la misma partición, preservando el orden cronológico estricto de llegada y balanceando equitativamente la carga.

> **P2: ¿Qué es el Target Leakage en este problema y por qué se descartaron `Happiness Rank` y `Dystopia Residual`?**  
> **Respuesta:** El *target leakage* ocurre cuando se incluyen variables que contienen directa o indirectamente el valor objetivo. `Happiness Rank` se obtiene ordenando el score; en streaming es imposible conocer el ranking de un país antes de predecir el resto del mundo. `Dystopia Residual` es matemáticamente $Score - \sum(\text{Factores})$; incluirlo degrada la regresión a una suma directa, destruyendo el aprendizaje predictivo real.

> **P3: ¿Por qué el consumidor guarda el mensaje en `raw_happiness_events` antes de validar e inferir?**  
> **Respuesta:** Implementa el patrón de ingeniería *Landing / Staging*. Guardar el payload crudo garantiza inmutabilidad, auditoría forense y tolerancia a fallos: ningún dato se pierde si falla la red o el formato. Además, habilita la *reproducibilidad* (replayability): si mañana se actualiza el modelo a una versión 2.0, los eventos crudos pueden re-procesarse sin retransmitirlos desde Kafka.

> **P4: ¿Por qué el dashboard de Power BI se conecta a PostgreSQL y no a los CSVs originales?**  
> **Respuesta:** En un pipeline de Streaming ETL en producción los datos arriban en tiempo real y deben consultarse desde el motor analítico para reflejar las inferencias y el estado vivo de la arquitectura. Conectarse a PostgreSQL aprovecha las llaves foráneas del Star Schema, los índices relacionales y la auditoría de ingesta, mientras que leer CSVs representaría un proceso batch desconectado.
