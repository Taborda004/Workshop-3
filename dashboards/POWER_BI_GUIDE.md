# GUÍA PASO A PASO: DASHBOARD PRECONSTRUIDO EN POWER BI DESKTOP
## Workshop-3: Streaming ETL con Apache Kafka & Machine Learning
**Conexión Directa:** PostgreSQL en `localhost:6432` / `happiness_db`

---

## 🚀 OPCIÓN 1: APERTURA AUTOMÁTICA EN 1 CLIC (PLANTILLA PRECONFIGURADA)

Para que no tengas que armar los gráficos manualmente desde un lienzo en blanco, hemos generado la **Plantilla Oficial de Power BI (.pbit)** y el **Proyecto PBIP** con todos los componentes visuales ya diseñados y posicionados:

📁 **Archivos generados en tu carpeta `dashboards/`:**
1. [`dashboards/ABRIR_DASHBOARD_POWER_BI.bat`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_3/dashboards/ABRIR_DASHBOARD_POWER_BI.bat) *(Lanzador directo en Windows)*
2. [`dashboards/Workshop3_Happiness_Dashboard.pbit`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_3/dashboards/Workshop3_Happiness_Dashboard.pbit) *(Plantilla de Power BI con todos los visuales y DAX)*
3. [`dashboards/Workshop3_Happiness.pbip`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_3/dashboards/Workshop3_Happiness.pbip) *(Proyecto PBIP nativo de Power BI)*

### ¿Cómo abrirlo?
1. Haz doble clic en [`dashboards/ABRIR_DASHBOARD_POWER_BI.bat`](file:///c:/Users/mauri/OneDrive/Escritorio/etl/workshop_3/dashboards/ABRIR_DASHBOARD_POWER_BI.bat) (o clic derecho en `Workshop3_Happiness_Dashboard.pbit` -> **Abrir con -> Power BI Desktop**).
2. Se abrirá **Power BI Desktop** cargando inmediatamente el lienzo con los gráficos y tarjetas ya configurados.
3. Si Power BI solicita credenciales de conexión para `localhost:6432`:
   - Selecciona la pestaña **Base de datos** (Database).
   - **Usuario:** `postgres`
   - **Contraseña:** `postgres`
   - Clic en **Conectar**.
4. Haz clic en **Cargar** (Load): ¡Listo! Todos los gráficos se poblarán instantáneamente con los datos en vivo de PostgreSQL.

---

## 🎨 ESTRUCTURA DEL DASHBOARD PREDISEÑADO

El lienzo de 1920x1080 incluye todos los elementos exigidos por la rúbrica y los 4 KPIs del PDF:

### 1. Fila Superior: Tarjetas de Métricas Globales (KPI Summary Cards)
- **Total Predictions:** `782` eventos históricos procesados por el pipeline de streaming.
- **Global MAE:** `0.2405` (Error Medio Absoluto).
- **Global RMSE:** `0.3441` (Raíz del Error Cuadrático Medio).
- **Avg Actual Score:** `5.378` (Promedio de Felicidad Real).
- **Avg Predicted Score:** `5.370` (Promedio de Felicidad Predicho por el modelo).
- **Prediction Bias:** `-0.008` (Sesgo cercano a cero, confirmando balance del modelo).

### 2. Fila Media: Análisis de Rendimiento y Tendencia
- **Gráfico de Columnas Agrupadas (KPI 1 - Izquierda):**
  - Muestra el **MAE y RMSE desglosado por Región Geográfica** (`dim_country.region`).
  - Permite identificar visualmente que Norteamérica y Europa Occidental tienen menor error, mientras que África Subsahariana presenta mayor variabilidad.
- **Gráfico de Líneas Temporal (KPI 4 - Derecha):**
  - Compara la curva del **Score Real vs Score Predicho a través de los años (2015 a 2019)** (`dim_date.year`).
  - Demuestra la estabilidad temporal de las inferencias y ausencia de *concept drift*.

### 3. Fila Inferior: Análisis de Residuos, Auditoría y Filtros
- **Tabla Detallada de Países y Residuos (KPI 2 - Izquierda):**
  - Lista detallada por país (`country_name`), región (`region`), score real (`actual_score`), predicción (`predicted_score`) y residuo (`prediction_error`).
  - Permite ordenar de mayor a menor discrepancia para auditar outliers.
- **Gráfico de Anillos de Tolerancia a Fallos (Auditoría Ingesta - Centro-Derecha):**
  - Muestra la distribución de estados de procesamiento en `raw_happiness_events`:
    - `782` eventos válidos (99.5%).
    - `4` eventos corruptos/incompletos interceptados exitosamente por la tolerancia a fallos (0.5%).
- **Segmentadores Interactivos (Slicers - Derecha):**
  - Filtro interactivo por **Región**.
  - Filtro interactivo por **Año**.

---

## ⚙️ DETALLES TÉCNICOS DEL MODELO SEMÁNTICO (STAR SCHEMA)

### Tablas y Relaciones:
```
       dim_country [1] ────────< [*] fact_predictions
       (country_id)                  (country_id)

       dim_date    [1] ────────< [*] fact_predictions
       (date_id)                     (date_id)

 raw_happiness_events [1] ─────── [1] fact_predictions
       (raw_event_id)                (raw_event_id)
```

### Medidas DAX Incluidas:
```dax
Total Predictions = COUNTROWS(fact_predictions)

MAE = SUMX(fact_predictions, ABS(fact_predictions[prediction_error])) / COUNTROWS(fact_predictions)

RMSE = SQRT(SUMX(fact_predictions, fact_predictions[prediction_error] * fact_predictions[prediction_error]) / COUNTROWS(fact_predictions))

Avg Actual Score = AVERAGE(fact_predictions[actual_score])

Avg Predicted Score = AVERAGE(fact_predictions[predicted_score])

Prediction Bias = [Avg Predicted Score] - [Avg Actual Score]
```

---

## 🎯 DEFENDIENDO ESTE DASHBOARD EN LA SUSTENTACIÓN ORAL (30% NOTA)

> **Pregunta Clave:** *¿Por qué el dashboard se conecta a PostgreSQL y no directamente al CSV original?*
>
> **Respuesta:** En un pipeline de Streaming ETL de producción, los eventos provienen de un broker de mensajería (Apache Kafka) en tiempo real, pasan por validación y enriquecimiento con inferencia ML, y se persisten en una base de datos relacional modelada en esquema en estrella. Conectar Power BI directamente a PostgreSQL asegura que los reportes reflejen el estado vivo de la arquitectura, aprovecha la integridad referencial y permite auditoría de tolerancia a fallos mediante la tabla cruda `raw_happiness_events`.
