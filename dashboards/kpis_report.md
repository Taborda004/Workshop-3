# LIVE POSTGRESQL KPI EXECUTION REPORT

## KPI 1.1 - Global Error Summary

|   total_predictions |   global_mae |   global_rmse |   mean_bias |   min_error |   max_error |
|--------------------:|-------------:|--------------:|------------:|------------:|------------:|
|                 782 |       0.2427 |        0.3434 |     -0.0002 |     -1.8149 |      1.2453 |



## KPI 1.2 - Error by Geographic Region

| region                          |   prediction_count |   region_mae |   region_rmse |   avg_actual_score |   avg_predicted_score |
|:--------------------------------|-------------------:|-------------:|--------------:|-------------------:|----------------------:|
| Australia and New Zealand       |                 10 |       0.0786 |        0.0935 |               7.29 |                  7.24 |
| North America                   |                 10 |       0.088  |        0.1135 |               7.17 |                  7.1  |
| Western Europe                  |                105 |       0.1997 |        0.2651 |               6.74 |                  6.71 |
| Middle East and Northern Africa |                 96 |       0.2109 |        0.3001 |               5.34 |                  5.4  |
| Central and Eastern Europe      |                145 |       0.2179 |        0.3029 |               5.43 |                  5.42 |
| Eastern Asia                    |                 30 |       0.247  |        0.3293 |               5.65 |                  5.86 |
| Sub-Saharan Africa              |                196 |       0.2574 |        0.3822 |               4.19 |                  4.27 |
| Southeastern Asia               |                 44 |       0.2605 |        0.3373 |               5.34 |                  5.37 |
| Latin America and Caribbean     |                111 |       0.3128 |        0.4159 |               6.02 |                  5.78 |
| Southern Asia                   |                 35 |       0.3229 |        0.4327 |               4.58 |                  4.66 |



## KPI 2 - Predictions by Country (Top 15 sample)

| country_name            | region                          |   total_predictions |   avg_actual_score |   avg_predicted_score |   country_mae |
|:------------------------|:--------------------------------|--------------------:|-------------------:|----------------------:|--------------:|
| Somalia                 | Sub-Saharan Africa              |                   6 |              5.059 |                 4.764 |         0.307 |
| Australia               | Australia and New Zealand       |                   5 |              7.276 |                 7.273 |         0.054 |
| Uruguay                 | Latin America and Caribbean     |                   5 |              6.431 |                 6.374 |         0.057 |
| Canada                  | North America                   |                   5 |              7.351 |                 7.287 |         0.064 |
| Congo (Kinshasa)        | Sub-Saharan Africa              |                   5 |              4.346 |                 4.288 |         0.065 |
| Spain                   | Western Europe                  |                   5 |              6.351 |                 6.279 |         0.072 |
| Turkey                  | Middle East and Northern Africa |                   5 |              5.415 |                 5.481 |         0.072 |
| Macedonia               | Central and Eastern Europe      |                   5 |              5.152 |                 5.205 |         0.074 |
| Jamaica                 | Latin America and Caribbean     |                   5 |              5.662 |                 5.614 |         0.074 |
| Poland                  | Central and Eastern Europe      |                   5 |              5.981 |                 6.058 |         0.077 |
| Kuwait                  | Middle East and Northern Africa |                   5 |              6.149 |                 6.228 |         0.079 |
| Montenegro              | Central and Eastern Europe      |                   5 |              5.292 |                 5.217 |         0.086 |
| Lebanon                 | Middle East and Northern Africa |                   5 |              5.15  |                 5.177 |         0.086 |
| South Korea             | Eastern Asia                    |                   5 |              5.885 |                 5.913 |         0.088 |
| Palestinian Territories | Middle East and Northern Africa |                   5 |              4.737 |                 4.796 |         0.088 |



## KPI 3 - Predicted vs Actual Score (Sample 10)

|   prediction_id | country_name   |   year |   actual_score |   predicted_score |   error_residual |   relative_error_pct | error_bracket   |
|----------------:|:---------------|-------:|---------------:|------------------:|-----------------:|---------------------:|:----------------|
|               1 | Algeria        |   2015 |          5.605 |             5.218 |            0.387 |                 6.91 | ACCEPTABLE      |
|               2 | Angola         |   2015 |          4.033 |             4.113 |           -0.08  |                 1.99 | HIGH_PRECISION  |
|               3 | Albania        |   2015 |          4.959 |             4.961 |           -0.002 |                 0.03 | HIGH_PRECISION  |
|               4 | Argentina      |   2015 |          6.574 |             6.292 |            0.282 |                 4.29 | ACCEPTABLE      |
|               5 | Afghanistan    |   2015 |          3.575 |             3.706 |           -0.131 |                 3.65 | HIGH_PRECISION  |
|               6 | Armenia        |   2015 |          4.35  |             4.455 |           -0.105 |                 2.42 | HIGH_PRECISION  |
|               7 | Austria        |   2015 |          7.2   |             7.157 |            0.043 |                 0.6  | HIGH_PRECISION  |
|               8 | Bangladesh     |   2015 |          4.694 |             4.587 |            0.107 |                 2.28 | HIGH_PRECISION  |
|               9 | Benin          |   2015 |          3.34  |             3.625 |           -0.285 |                 8.54 | ACCEPTABLE      |
|              10 | Brazil         |   2015 |          6.983 |             6.549 |            0.434 |                 6.21 | ACCEPTABLE      |



## KPI 4 - Prediction Trends Over Time (By Year)

|   year |   events_processed |   avg_actual_score |   avg_predicted_score |   yearly_mae |   yearly_rmse |
|-------:|-------------------:|-------------------:|----------------------:|-------------:|--------------:|
|   2015 |                158 |              5.376 |                 5.35  |       0.2678 |        0.3677 |
|   2016 |                157 |              5.382 |                 5.296 |       0.2444 |        0.3556 |
|   2017 |                155 |              5.354 |                 5.414 |       0.2257 |        0.3276 |
|   2018 |                156 |              5.376 |                 5.392 |       0.2223 |        0.3142 |
|   2019 |                156 |              5.407 |                 5.446 |       0.2531 |        0.3483 |



## Raw Ingestion Audit & Fault Tolerance

| processing_status   |   total_events |   percentage |
|:--------------------|---------------:|-------------:|
| VALID               |            782 |        99.49 |
| INVALID_SCHEMA      |              2 |         0.25 |
| INVALID_VALUES      |              2 |         0.25 |


