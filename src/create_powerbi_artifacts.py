"""
Power BI Dashboard Generator for Workshop 3.
Generates:
1. dashboards/Workshop3_Happiness_Dashboard.pbit
2. dashboards/Workshop3_Happiness.pbip
3. Copies Workshop3_Happiness_Dashboard.pbit directly to Desktop (C:\\Users\\mauri\\OneDrive\\Escritorio)
"""

import json
import os
import shutil
import zipfile

BASE_DIR = r"c:\Users\mauri\OneDrive\Escritorio\etl\workshop_3\dashboards"
DESKTOP_DIR = r"c:\Users\mauri\OneDrive\Escritorio"
PBIP_NAME = "Workshop3_Happiness"
REPORT_DIR = os.path.join(BASE_DIR, f"{PBIP_NAME}.Report")
SEMANTIC_DIR = os.path.join(BASE_DIR, f"{PBIP_NAME}.SemanticModel")
TMDL_DIR = os.path.join(SEMANTIC_DIR, "definition")
TABLES_DIR = os.path.join(TMDL_DIR, "tables")
REPORT_DEF_DIR = os.path.join(REPORT_DIR, "definition")
PAGES_DIR = os.path.join(REPORT_DEF_DIR, "pages")
PAGE_DIR = os.path.join(PAGES_DIR, "page_kpi_overview")
VISUALS_DIR = os.path.join(PAGE_DIR, "visuals")

for d in [REPORT_DIR, SEMANTIC_DIR]:
    if os.path.exists(d):
        shutil.rmtree(d, ignore_errors=True)

os.makedirs(TABLES_DIR, exist_ok=True)
os.makedirs(VISUALS_DIR, exist_ok=True)

# 1. PBIP Pointer File
with open(os.path.join(BASE_DIR, f"{PBIP_NAME}.pbip"), "w", encoding="utf-8") as f:
    json.dump({
        "version": "1.0",
        "artifacts": [{"report": {"path": f"{PBIP_NAME}.Report"}}]
    }, f, indent=2)

# 2. TMDL Model
with open(os.path.join(SEMANTIC_DIR, "definition.pbism"), "w", encoding="utf-8") as f:
    json.dump({"version": "4.0"}, f, indent=2)

with open(os.path.join(TMDL_DIR, "database.tmdl"), "w", encoding="utf-8") as f:
    f.write("database\n\tcompatibilityLevel: 1567\n")

with open(os.path.join(TMDL_DIR, "model.tmdl"), "w", encoding="utf-8") as f:
    f.write('''model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tsourceQueryCulture: en-US
\tdataAccessOptions
\t\tlegacyRedirects
\t\treturnErrorValuesAsNull

annotation PBI_QueryOrder = ["dim_country","dim_date","fact_predictions","raw_happiness_events"]
annotation __PBI_TimeIntelligenceEnabled = 1
annotation PBIDesktopVersion = 2.159.1231.0 (24.08)
''')

# TMDL Tables
with open(os.path.join(TABLES_DIR, "dim_country.tmdl"), "w", encoding="utf-8") as f:
    f.write('''table dim_country
\tlineageTag: 5f9e2e6c-38d5-4519-86bb-bcf979f45e21

\tcolumn country_id
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: c2f4955b-4395-46aa-b2b9-e457fbc4e749
\t\tsummarizeBy: none
\t\tsourceColumn: country_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn country_name
\t\tdataType: string
\t\tlineageTag: a03e05a5-c3c7-4340-a3ce-b1efb2326ce8
\t\tsummarizeBy: none
\t\tsourceColumn: country_name
\t\tannotation SummarizationSetBy = Automatic

\tcolumn region
\t\tdataType: string
\t\tlineageTag: 72a27521-4f16-4fba-bb5c-15a0c30a8c27
\t\tsummarizeBy: none
\t\tsourceColumn: region
\t\tannotation SummarizationSetBy = Automatic

\tcolumn created_at
\t\tdataType: dateTime
\t\tformatString: General Date
\t\tlineageTag: 72a27521-4f16-4fba-bb5c-15a0c30a8c28
\t\tsummarizeBy: none
\t\tsourceColumn: created_at
\t\tannotation SummarizationSetBy = Automatic

\tpartition dim_country = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database("localhost:6432", "happiness_db"),
\t\t\t\tpublic_dim_country = Source{[Schema="public",Item="dim_country"]}[Data]
\t\t\tin
\t\t\t\tpublic_dim_country

\tannotation PBI_ResultType = Table
''')

with open(os.path.join(TABLES_DIR, "dim_date.tmdl"), "w", encoding="utf-8") as f:
    f.write('''table dim_date
\tlineageTag: 6c28fce8-0e31-41fb-9e8c-8be9e5f3aa41

\tcolumn date_id
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: 367ae68e-20fa-40f9-a299-138383822cf5
\t\tsummarizeBy: none
\t\tsourceColumn: date_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn year
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: 927a78e4-84d9-43c2-a9b0-9fbb34e15cf2
\t\tsummarizeBy: none
\t\tsourceColumn: year
\t\tannotation SummarizationSetBy = Automatic

\tcolumn event_time
\t\tdataType: dateTime
\t\tformatString: General Date
\t\tlineageTag: 48b3cbe8-5d25-4122-bd53-0b04c86ce3c8
\t\tsummarizeBy: none
\t\tsourceColumn: event_time
\t\tannotation SummarizationSetBy = Automatic

\tcolumn month
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: 48b3cbe8-5d25-4122-bd53-0b04c86ce3c0
\t\tsummarizeBy: none
\t\tsourceColumn: month
\t\tannotation SummarizationSetBy = Automatic

\tcolumn day
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: 48b3cbe8-5d25-4122-bd53-0b04c86ce3c1
\t\tsummarizeBy: none
\t\tsourceColumn: day
\t\tannotation SummarizationSetBy = Automatic

\tcolumn hour
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: 48b3cbe8-5d25-4122-bd53-0b04c86ce3c2
\t\tsummarizeBy: none
\t\tsourceColumn: hour
\t\tannotation SummarizationSetBy = Automatic

\tcolumn minute
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: 48b3cbe8-5d25-4122-bd53-0b04c86ce3c3
\t\tsummarizeBy: none
\t\tsourceColumn: minute
\t\tannotation SummarizationSetBy = Automatic

\tcolumn created_at
\t\tdataType: dateTime
\t\tformatString: General Date
\t\tlineageTag: 48b3cbe8-5d25-4122-bd53-0b04c86ce3c4
\t\tsummarizeBy: none
\t\tsourceColumn: created_at
\t\tannotation SummarizationSetBy = Automatic

\tpartition dim_date = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database("localhost:6432", "happiness_db"),
\t\t\t\tpublic_dim_date = Source{[Schema="public",Item="dim_date"]}[Data]
\t\t\tin
\t\t\t\tpublic_dim_date

\tannotation PBI_ResultType = Table
''')

with open(os.path.join(TABLES_DIR, "fact_predictions.tmdl"), "w", encoding="utf-8") as f:
    f.write('''table fact_predictions
\tlineageTag: b38e92d7-b89e-4c74-a63e-7f61c6bfe76b

\tmeasure 'Total Predictions' = COUNTROWS(fact_predictions)
\t\tformatString: #,0
\t\tlineageTag: e83cf8d1-58e1-4566-a67b-f111818d6e32

\tmeasure MAE = SUMX(fact_predictions, ABS(fact_predictions[prediction_error])) / COUNTROWS(fact_predictions)
\t\tformatString: 0.0000
\t\tlineageTag: f84cf8d1-58e1-4566-a67b-f111818d6e33

\tmeasure RMSE = SQRT(SUMX(fact_predictions, fact_predictions[prediction_error] * fact_predictions[prediction_error]) / COUNTROWS(fact_predictions))
\t\tformatString: 0.0000
\t\tlineageTag: a14cf8d1-58e1-4566-a67b-f111818d6e34

\tmeasure 'Avg Actual Score' = AVERAGE(fact_predictions[actual_score])
\t\tformatString: 0.000
\t\tlineageTag: b24cf8d1-58e1-4566-a67b-f111818d6e35

\tmeasure 'Avg Predicted Score' = AVERAGE(fact_predictions[predicted_score])
\t\tformatString: 0.000
\t\tlineageTag: c34cf8d1-58e1-4566-a67b-f111818d6e36

\tmeasure 'Prediction Bias' = [Avg Predicted Score] - [Avg Actual Score]
\t\tformatString: +0.0000;-0.0000;0.0000
\t\tlineageTag: d44cf8d1-58e1-4566-a67b-f111818d6e37

\tcolumn prediction_id
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c01
\t\tsummarizeBy: count
\t\tsourceColumn: prediction_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn raw_event_id
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c00
\t\tsummarizeBy: none
\t\tsourceColumn: raw_event_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn country_id
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c02
\t\tsummarizeBy: none
\t\tsourceColumn: country_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn date_id
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c03
\t\tsummarizeBy: none
\t\tsourceColumn: date_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn gdp
\t\tdataType: double
\t\tformatString: 0.0000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c04
\t\tsummarizeBy: sum
\t\tsourceColumn: gdp
\t\tannotation SummarizationSetBy = Automatic

\tcolumn family
\t\tdataType: double
\t\tformatString: 0.0000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c05
\t\tsummarizeBy: sum
\t\tsourceColumn: family
\t\tannotation SummarizationSetBy = Automatic

\tcolumn health
\t\tdataType: double
\t\tformatString: 0.0000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c06
\t\tsummarizeBy: sum
\t\tsourceColumn: health
\t\tannotation SummarizationSetBy = Automatic

\tcolumn freedom
\t\tdataType: double
\t\tformatString: 0.0000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c07
\t\tsummarizeBy: sum
\t\tsourceColumn: freedom
\t\tannotation SummarizationSetBy = Automatic

\tcolumn generosity
\t\tdataType: double
\t\tformatString: 0.0000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c08
\t\tsummarizeBy: sum
\t\tsourceColumn: generosity
\t\tannotation SummarizationSetBy = Automatic

\tcolumn corruption
\t\tdataType: double
\t\tformatString: 0.0000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c09
\t\tsummarizeBy: sum
\t\tsourceColumn: corruption
\t\tannotation SummarizationSetBy = Automatic

\tcolumn actual_score
\t\tdataType: double
\t\tformatString: 0.000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c10
\t\tsummarizeBy: average
\t\tsourceColumn: actual_score
\t\tannotation SummarizationSetBy = Automatic

\tcolumn predicted_score
\t\tdataType: double
\t\tformatString: 0.000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c11
\t\tsummarizeBy: average
\t\tsourceColumn: predicted_score
\t\tannotation SummarizationSetBy = Automatic

\tcolumn prediction_error
\t\tdataType: double
\t\tformatString: 0.0000
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c12
\t\tsummarizeBy: average
\t\tsourceColumn: prediction_error
\t\tannotation SummarizationSetBy = Automatic

\tcolumn event_time
\t\tdataType: dateTime
\t\tformatString: General Date
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c13
\t\tsummarizeBy: none
\t\tsourceColumn: event_time
\t\tannotation SummarizationSetBy = Automatic

\tcolumn prediction_timestamp
\t\tdataType: dateTime
\t\tformatString: General Date
\t\tlineageTag: d02ea68e-20fa-40f9-a299-138383822c14
\t\tsummarizeBy: none
\t\tsourceColumn: prediction_timestamp
\t\tannotation SummarizationSetBy = Automatic

\tpartition fact_predictions = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database("localhost:6432", "happiness_db"),
\t\t\t\tpublic_fact_predictions = Source{[Schema="public",Item="fact_predictions"]}[Data]
\t\t\tin
\t\t\t\tpublic_fact_predictions

\tannotation PBI_ResultType = Table
''')

with open(os.path.join(TABLES_DIR, "raw_happiness_events.tmdl"), "w", encoding="utf-8") as f:
    f.write('''table raw_happiness_events
\tlineageTag: c48e92d7-b89e-4c74-a63e-7f61c6bfe76c

\tcolumn raw_event_id
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c20
\t\tsummarizeBy: count
\t\tsourceColumn: raw_event_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn event_id
\t\tdataType: string
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c19
\t\tsummarizeBy: none
\t\tsourceColumn: event_id
\t\tannotation SummarizationSetBy = Automatic

\tcolumn country
\t\tdataType: string
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c18
\t\tsummarizeBy: none
\t\tsourceColumn: country
\t\tannotation SummarizationSetBy = Automatic

\tcolumn year
\t\tdataType: int64
\t\tformatString: 0
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c17
\t\tsummarizeBy: none
\t\tsourceColumn: year
\t\tannotation SummarizationSetBy = Automatic

\tcolumn raw_payload
\t\tdataType: string
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c21
\t\tsummarizeBy: none
\t\tsourceColumn: raw_payload
\t\tannotation SummarizationSetBy = Automatic

\tcolumn processing_status
\t\tdataType: string
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c23
\t\tsummarizeBy: none
\t\tsourceColumn: processing_status
\t\tannotation SummarizationSetBy = Automatic

\tcolumn error_message
\t\tdataType: string
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c24
\t\tsummarizeBy: none
\t\tsourceColumn: error_message
\t\tannotation SummarizationSetBy = Automatic

\tcolumn ingestion_timestamp
\t\tdataType: dateTime
\t\tformatString: General Date
\t\tlineageTag: e02ea68e-20fa-40f9-a299-138383822c22
\t\tsummarizeBy: none
\t\tsourceColumn: ingestion_timestamp
\t\tannotation SummarizationSetBy = Automatic

\tpartition raw_happiness_events = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t\tSource = PostgreSQL.Database("localhost:6432", "happiness_db"),
\t\t\t\tpublic_raw_happiness_events = Source{[Schema="public",Item="raw_happiness_events"]}[Data]
\t\t\tin
\t\t\t\tpublic_raw_happiness_events

\tannotation PBI_ResultType = Table
''')

with open(os.path.join(TMDL_DIR, "relationships.tmdl"), "w", encoding="utf-8") as f:
    f.write('''relationship AutoDetected_fact_predictions_country_id_dim_country
\tfromColumn: fact_predictions.country_id
\ttoColumn: dim_country.country_id

relationship AutoDetected_fact_predictions_date_id_dim_date
\tfromColumn: fact_predictions.date_id
\ttoColumn: dim_date.date_id
''')

# 3. Report PBIR Definition
with open(os.path.join(REPORT_DIR, "definition.pbir"), "w", encoding="utf-8") as f:
    json.dump({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pbir/1.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byPath": {"path": f"../{PBIP_NAME}.SemanticModel"},
            "byConnection": None
        }
    }, f, indent=2)

with open(os.path.join(REPORT_DEF_DIR, "version.json"), "w", encoding="utf-8") as f:
    json.dump({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "2.0.0"
    }, f, indent=2)

static_dest = os.path.join(REPORT_DIR, "StaticResources")
with zipfile.ZipFile(r"C:\Users\mauri\OneDrive\Escritorio\etl\workshop_2\workshop-2.pbix", "r") as z:
    for name in z.namelist():
        if name.startswith("Report/StaticResources/"):
            rel_path = name.replace("Report/StaticResources/", "")
            out_file = os.path.join(static_dest, rel_path)
            os.makedirs(os.path.dirname(out_file), exist_ok=True)
            with open(out_file, "wb") as out_f:
                out_f.write(z.read(name))

with open(os.path.join(REPORT_DEF_DIR, "report.json"), "w", encoding="utf-8") as f:
    json.dump({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/3.3.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": "Fluent2-CY26SU09",
                "reportVersionAtImport": {"visual": "2.13.0", "report": "3.4.0", "page": "2.3.1"},
                "type": "SharedResources"
            },
            "customTheme": {
                "name": "Tidal",
                "reportVersionAtImport": {"visual": "2.13.0", "report": "3.4.0", "page": "2.3.1"},
                "type": "SharedResources"
            }
        }
    }, f, indent=2)

with open(os.path.join(PAGES_DIR, "pages.json"), "w", encoding="utf-8") as f:
    json.dump({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
        "pageOrder": ["page_kpi_overview"],
        "activePageName": "page_kpi_overview"
    }, f, indent=2)

with open(os.path.join(PAGE_DIR, "page.json"), "w", encoding="utf-8") as f:
    json.dump({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json",
        "name": "page_kpi_overview",
        "displayName": "Workshop 3: ML Happiness Analytics",
        "displayOption": "FitToPage",
        "height": 1080,
        "width": 1920
    }, f, indent=2)

def write_visual(v_name, visual_dict):
    v_subdir = os.path.join(VISUALS_DIR, v_name)
    os.makedirs(v_subdir, exist_ok=True)
    p = os.path.join(v_subdir, "visual.json")
    with open(p, "w", encoding="utf-8") as out:
        json.dump(visual_dict, out, indent=2)

# Cards
write_visual("card_total_predictions", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "card_total_predictions",
    "position": {"x": 20, "y": 80, "z": 100, "height": 120, "width": 300, "tabOrder": 100},
    "visual": {
        "visualType": "cardVisual",
        "query": {
            "queryState": {
                "Data": {
                    "projections": [{
                        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "Total Predictions"}},
                        "queryRef": "fact_predictions.Total Predictions",
                        "nativeQueryRef": "Total Predictions"
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

write_visual("card_global_mae", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "card_global_mae",
    "position": {"x": 340, "y": 80, "z": 101, "height": 120, "width": 300, "tabOrder": 101},
    "visual": {
        "visualType": "cardVisual",
        "query": {
            "queryState": {
                "Data": {
                    "projections": [{
                        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "MAE"}},
                        "queryRef": "fact_predictions.MAE",
                        "nativeQueryRef": "MAE"
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

write_visual("card_global_rmse", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "card_global_rmse",
    "position": {"x": 660, "y": 80, "z": 102, "height": 120, "width": 300, "tabOrder": 102},
    "visual": {
        "visualType": "cardVisual",
        "query": {
            "queryState": {
                "Data": {
                    "projections": [{
                        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "RMSE"}},
                        "queryRef": "fact_predictions.RMSE",
                        "nativeQueryRef": "RMSE"
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

write_visual("card_avg_actual", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "card_avg_actual",
    "position": {"x": 980, "y": 80, "z": 103, "height": 120, "width": 300, "tabOrder": 103},
    "visual": {
        "visualType": "cardVisual",
        "query": {
            "queryState": {
                "Data": {
                    "projections": [{
                        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "Avg Actual Score"}},
                        "queryRef": "fact_predictions.Avg Actual Score",
                        "nativeQueryRef": "Avg Actual Score"
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

write_visual("card_avg_predicted", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "card_avg_predicted",
    "position": {"x": 1300, "y": 80, "z": 104, "height": 120, "width": 300, "tabOrder": 104},
    "visual": {
        "visualType": "cardVisual",
        "query": {
            "queryState": {
                "Data": {
                    "projections": [{
                        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "Avg Predicted Score"}},
                        "queryRef": "fact_predictions.Avg Predicted Score",
                        "nativeQueryRef": "Avg Predicted Score"
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

write_visual("card_prediction_bias", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "card_prediction_bias",
    "position": {"x": 1620, "y": 80, "z": 105, "height": 120, "width": 280, "tabOrder": 105},
    "visual": {
        "visualType": "cardVisual",
        "query": {
            "queryState": {
                "Data": {
                    "projections": [{
                        "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "Prediction Bias"}},
                        "queryRef": "fact_predictions.Prediction Bias",
                        "nativeQueryRef": "Prediction Bias"
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

# KPI 1 Clustered Column
write_visual("chart_kpi1_regional_metrics", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "chart_kpi1_regional_metrics",
    "position": {"x": 20, "y": 220, "z": 200, "height": 400, "width": 930, "tabOrder": 200},
    "visual": {
        "visualType": "clusteredColumnChart",
        "query": {
            "queryState": {
                "Category": {
                    "projections": [{
                        "field": {"Column": {"Expression": {"SourceRef": {"Entity": "dim_country"}}, "Property": "region"}},
                        "queryRef": "dim_country.region",
                        "nativeQueryRef": "region",
                        "active": True
                    }]
                },
                "Y": {
                    "projections": [
                        {
                            "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "MAE"}},
                            "queryRef": "fact_predictions.MAE",
                            "nativeQueryRef": "MAE"
                        },
                        {
                            "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "RMSE"}},
                            "queryRef": "fact_predictions.RMSE",
                            "nativeQueryRef": "RMSE"
                        }
                    ]
                }
            },
            "sortDefinition": {
                "sort": [{"field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "MAE"}}, "direction": "Descending"}],
                "isDefaultSort": True
            }
        },
        "drillFilterOtherVisuals": True
    }
})

# KPI 4 Line Chart
write_visual("chart_kpi4_temporal_evolution", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "chart_kpi4_temporal_evolution",
    "position": {"x": 970, "y": 220, "z": 201, "height": 400, "width": 930, "tabOrder": 201},
    "visual": {
        "visualType": "lineChart",
        "query": {
            "queryState": {
                "Category": {
                    "projections": [{
                        "field": {"Column": {"Expression": {"SourceRef": {"Entity": "dim_date"}}, "Property": "year"}},
                        "queryRef": "dim_date.year",
                        "nativeQueryRef": "year",
                        "active": True
                    }]
                },
                "Y": {
                    "projections": [
                        {
                            "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "Avg Actual Score"}},
                            "queryRef": "fact_predictions.Avg Actual Score",
                            "nativeQueryRef": "Avg Actual Score"
                        },
                        {
                            "field": {"Measure": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "Avg Predicted Score"}},
                            "queryRef": "fact_predictions.Avg Predicted Score",
                            "nativeQueryRef": "Avg Predicted Score"
                        }
                    ]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

# KPI 2 Table
write_visual("table_kpi2_country_residuals", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "table_kpi2_country_residuals",
    "position": {"x": 20, "y": 640, "z": 300, "height": 410, "width": 930, "tabOrder": 300},
    "visual": {
        "visualType": "tableEx",
        "query": {
            "queryState": {
                "Values": {
                    "projections": [
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "dim_country"}}, "Property": "country_name"}}, "queryRef": "dim_country.country_name", "nativeQueryRef": "country_name"},
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "dim_country"}}, "Property": "region"}}, "queryRef": "dim_country.region", "nativeQueryRef": "region"},
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "actual_score"}}, "queryRef": "fact_predictions.actual_score", "nativeQueryRef": "actual_score"},
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "predicted_score"}}, "queryRef": "fact_predictions.predicted_score", "nativeQueryRef": "predicted_score"},
                        {"field": {"Column": {"Expression": {"SourceRef": {"Entity": "fact_predictions"}}, "Property": "prediction_error"}}, "queryRef": "fact_predictions.prediction_error", "nativeQueryRef": "prediction_error"}
                    ]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

# Audit Donut
write_visual("donut_audit_resilience", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "donut_audit_resilience",
    "position": {"x": 970, "y": 640, "z": 301, "height": 410, "width": 480, "tabOrder": 301},
    "visual": {
        "visualType": "donutChart",
        "query": {
            "queryState": {
                "Category": {
                    "projections": [{
                        "field": {"Column": {"Expression": {"SourceRef": {"Entity": "raw_happiness_events"}}, "Property": "processing_status"}},
                        "queryRef": "raw_happiness_events.processing_status",
                        "nativeQueryRef": "processing_status",
                        "active": True
                    }]
                },
                "Y": {
                    "projections": [{
                        "field": {"Column": {"Expression": {"SourceRef": {"Entity": "raw_happiness_events"}}, "Property": "raw_event_id"}},
                        "queryRef": "CountNonNull(raw_happiness_events.raw_event_id)",
                        "nativeQueryRef": "Count of raw_event_id"
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

# Slicers
write_visual("slicer_region", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "slicer_region",
    "position": {"x": 1470, "y": 640, "z": 302, "height": 410, "width": 220, "tabOrder": 302},
    "visual": {
        "visualType": "slicer",
        "query": {
            "queryState": {
                "Values": {
                    "projections": [{
                        "field": {"Column": {"Expression": {"SourceRef": {"Entity": "dim_country"}}, "Property": "region"}},
                        "queryRef": "dim_country.region",
                        "nativeQueryRef": "region",
                        "active": True
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

write_visual("slicer_year", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.12.0/schema.json",
    "name": "slicer_year",
    "position": {"x": 1710, "y": 640, "z": 303, "height": 410, "width": 190, "tabOrder": 303},
    "visual": {
        "visualType": "slicer",
        "query": {
            "queryState": {
                "Values": {
                    "projections": [{
                        "field": {"Column": {"Expression": {"SourceRef": {"Entity": "dim_date"}}, "Property": "year"}},
                        "queryRef": "dim_date.year",
                        "nativeQueryRef": "year",
                        "active": True
                    }]
                }
            }
        },
        "drillFilterOtherVisuals": True
    }
})

# 4. DiagramLayout
diagram_layout = {
    "version": "1.1.0",
    "diagrams": [{
        "ordinal": 0,
        "scrollPosition": {"x": 0, "y": 0},
        "nodes": [
            {"location": {"x": 50, "y": 150}, "nodeIndex": "dim_country", "nodeLineageTag": "5f9e2e6c-38d5-4519-86bb-bcf979f45e21", "size": {"height": 220, "width": 230}, "zIndex": 0},
            {"location": {"x": 50, "y": 420}, "nodeIndex": "dim_date", "nodeLineageTag": "6c28fce8-0e31-41fb-9e8c-8be9e5f3aa41", "size": {"height": 240, "width": 230}, "zIndex": 0},
            {"location": {"x": 380, "y": 200}, "nodeIndex": "fact_predictions", "nodeLineageTag": "b38e92d7-b89e-4c74-a63e-7f61c6bfe76b", "size": {"height": 380, "width": 260}, "zIndex": 0},
            {"location": {"x": 720, "y": 200}, "nodeIndex": "raw_happiness_events", "nodeLineageTag": "c48e92d7-b89e-4c74-a63e-7f61c6bfe76c", "size": {"height": 280, "width": 250}, "zIndex": 0}
        ],
        "name": "Todas las tablas",
        "zoomValue": 100.0,
        "pinKeyFieldsToTop": False,
        "showExtraHeaderInfo": False,
        "hideKeyFieldsWhenCollapsed": False,
        "tablesLocked": False
    }],
    "selectedDiagram": "Todas las tablas",
    "defaultDiagram": "Todas las tablas"
}

# 5. PBIT Template File
pbit_path = os.path.join(BASE_DIR, "Workshop3_Happiness_Dashboard.pbit")

tmsl_model = {
    "compatibilityLevel": 1567,
    "model": {
        "culture": "en-US",
        "dataAccessOptions": {"legacyRedirects": True, "returnErrorValuesAsNull": True},
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "sourceQueryCulture": "en-US",
        "tables": [
            {
                "name": "dim_country",
                "lineageTag": "5f9e2e6c-38d5-4519-86bb-bcf979f45e21",
                "columns": [
                    {"name": "country_id", "dataType": "int64", "formatString": "0", "sourceColumn": "country_id", "summarizeBy": "none"},
                    {"name": "country_name", "dataType": "string", "sourceColumn": "country_name", "summarizeBy": "none"},
                    {"name": "region", "dataType": "string", "sourceColumn": "region", "summarizeBy": "none"},
                    {"name": "created_at", "dataType": "dateTime", "formatString": "General Date", "sourceColumn": "created_at", "summarizeBy": "none"}
                ],
                "partitions": [{
                    "name": "dim_country",
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": [
                            "let",
                            "    Source = PostgreSQL.Database(\"localhost:6432\", \"happiness_db\"),",
                            "    public_dim_country = Source{[Schema=\"public\",Item=\"dim_country\"]}[Data]",
                            "in",
                            "    public_dim_country"
                        ]
                    }
                }]
            },
            {
                "name": "dim_date",
                "lineageTag": "6c28fce8-0e31-41fb-9e8c-8be9e5f3aa41",
                "columns": [
                    {"name": "date_id", "dataType": "int64", "formatString": "0", "sourceColumn": "date_id", "summarizeBy": "none"},
                    {"name": "year", "dataType": "int64", "formatString": "0", "sourceColumn": "year", "summarizeBy": "none"},
                    {"name": "event_time", "dataType": "dateTime", "formatString": "General Date", "sourceColumn": "event_time", "summarizeBy": "none"},
                    {"name": "month", "dataType": "int64", "formatString": "0", "sourceColumn": "month", "summarizeBy": "none"},
                    {"name": "day", "dataType": "int64", "formatString": "0", "sourceColumn": "day", "summarizeBy": "none"},
                    {"name": "hour", "dataType": "int64", "formatString": "0", "sourceColumn": "hour", "summarizeBy": "none"},
                    {"name": "minute", "dataType": "int64", "formatString": "0", "sourceColumn": "minute", "summarizeBy": "none"},
                    {"name": "created_at", "dataType": "dateTime", "formatString": "General Date", "sourceColumn": "created_at", "summarizeBy": "none"}
                ],
                "partitions": [{
                    "name": "dim_date",
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": [
                            "let",
                            "    Source = PostgreSQL.Database(\"localhost:6432\", \"happiness_db\"),",
                            "    public_dim_date = Source{[Schema=\"public\",Item=\"dim_date\"]}[Data]",
                            "in",
                            "    public_dim_date"
                        ]
                    }
                }]
            },
            {
                "name": "fact_predictions",
                "lineageTag": "b38e92d7-b89e-4c74-a63e-7f61c6bfe76b",
                "measures": [
                    {"name": "Total Predictions", "expression": "COUNTROWS(fact_predictions)", "formatString": "#,0"},
                    {"name": "MAE", "expression": "SUMX(fact_predictions, ABS(fact_predictions[prediction_error])) / COUNTROWS(fact_predictions)", "formatString": "0.0000"},
                    {"name": "RMSE", "expression": "SQRT(SUMX(fact_predictions, fact_predictions[prediction_error] * fact_predictions[prediction_error]) / COUNTROWS(fact_predictions))", "formatString": "0.0000"},
                    {"name": "Avg Actual Score", "expression": "AVERAGE(fact_predictions[actual_score])", "formatString": "0.000"},
                    {"name": "Avg Predicted Score", "expression": "AVERAGE(fact_predictions[predicted_score])", "formatString": "0.000"},
                    {"name": "Prediction Bias", "expression": "[Avg Predicted Score] - [Avg Actual Score]", "formatString": "+0.0000;-0.0000;0.0000"}
                ],
                "columns": [
                    {"name": "prediction_id", "dataType": "int64", "formatString": "0", "sourceColumn": "prediction_id", "summarizeBy": "count"},
                    {"name": "raw_event_id", "dataType": "int64", "formatString": "0", "sourceColumn": "raw_event_id", "summarizeBy": "none"},
                    {"name": "country_id", "dataType": "int64", "formatString": "0", "sourceColumn": "country_id", "summarizeBy": "none"},
                    {"name": "date_id", "dataType": "int64", "formatString": "0", "sourceColumn": "date_id", "summarizeBy": "none"},
                    {"name": "gdp", "dataType": "double", "formatString": "0.0000", "sourceColumn": "gdp", "summarizeBy": "sum"},
                    {"name": "family", "dataType": "double", "formatString": "0.0000", "sourceColumn": "family", "summarizeBy": "sum"},
                    {"name": "health", "dataType": "double", "formatString": "0.0000", "sourceColumn": "health", "summarizeBy": "sum"},
                    {"name": "freedom", "dataType": "double", "formatString": "0.0000", "sourceColumn": "freedom", "summarizeBy": "sum"},
                    {"name": "generosity", "dataType": "double", "formatString": "0.0000", "sourceColumn": "generosity", "summarizeBy": "sum"},
                    {"name": "corruption", "dataType": "double", "formatString": "0.0000", "sourceColumn": "corruption", "summarizeBy": "sum"},
                    {"name": "actual_score", "dataType": "double", "formatString": "0.000", "sourceColumn": "actual_score", "summarizeBy": "average"},
                    {"name": "predicted_score", "dataType": "double", "formatString": "0.000", "sourceColumn": "predicted_score", "summarizeBy": "average"},
                    {"name": "prediction_error", "dataType": "double", "formatString": "0.0000", "sourceColumn": "prediction_error", "summarizeBy": "average"},
                    {"name": "event_time", "dataType": "dateTime", "formatString": "General Date", "sourceColumn": "event_time", "summarizeBy": "none"},
                    {"name": "prediction_timestamp", "dataType": "dateTime", "formatString": "General Date", "sourceColumn": "prediction_timestamp", "summarizeBy": "none"}
                ],
                "partitions": [{
                    "name": "fact_predictions",
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": [
                            "let",
                            "    Source = PostgreSQL.Database(\"localhost:6432\", \"happiness_db\"),",
                            "    public_fact_predictions = Source{[Schema=\"public\",Item=\"fact_predictions\"]}[Data]",
                            "in",
                            "    public_fact_predictions"
                        ]
                    }
                }]
            },
            {
                "name": "raw_happiness_events",
                "lineageTag": "c48e92d7-b89e-4c74-a63e-7f61c6bfe76c",
                "columns": [
                    {"name": "raw_event_id", "dataType": "int64", "formatString": "0", "sourceColumn": "raw_event_id", "summarizeBy": "count"},
                    {"name": "event_id", "dataType": "string", "sourceColumn": "event_id", "summarizeBy": "none"},
                    {"name": "country", "dataType": "string", "sourceColumn": "country", "summarizeBy": "none"},
                    {"name": "year", "dataType": "int64", "formatString": "0", "sourceColumn": "year", "summarizeBy": "none"},
                    {"name": "raw_payload", "dataType": "string", "sourceColumn": "raw_payload", "summarizeBy": "none"},
                    {"name": "processing_status", "dataType": "string", "sourceColumn": "processing_status", "summarizeBy": "none"},
                    {"name": "error_message", "dataType": "string", "sourceColumn": "error_message", "summarizeBy": "none"},
                    {"name": "ingestion_timestamp", "dataType": "dateTime", "formatString": "General Date", "sourceColumn": "ingestion_timestamp", "summarizeBy": "none"}
                ],
                "partitions": [{
                    "name": "raw_happiness_events",
                    "mode": "import",
                    "source": {
                        "type": "m",
                        "expression": [
                            "let",
                            "    Source = PostgreSQL.Database(\"localhost:6432\", \"happiness_db\"),",
                            "    public_raw_happiness_events = Source{[Schema=\"public\",Item=\"raw_happiness_events\"]}[Data]",
                            "in",
                            "    public_raw_happiness_events"
                        ]
                    }
                }]
            }
        ],
        "relationships": [
            {
                "name": "rel_fact_country",
                "fromTable": "fact_predictions",
                "fromColumn": "country_id",
                "toTable": "dim_country",
                "toColumn": "country_id"
            },
            {
                "name": "rel_fact_date",
                "fromTable": "fact_predictions",
                "fromColumn": "date_id",
                "toTable": "dim_date",
                "toColumn": "date_id"
            }
        ]
    }
}

content_types_xml = '<?xml version="1.0" encoding="utf-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="json" ContentType="" /><Override PartName="/Version" ContentType="" /><Override PartName="/DiagramLayout" ContentType="" /><Override PartName="/Settings" ContentType="application/json" /><Override PartName="/Metadata" ContentType="application/json" /><Override PartName="/DataModelSchema" ContentType="" /></Types>'

with zipfile.ZipFile(pbit_path, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("[Content_Types].xml", content_types_xml.encode("utf-8"))
    z.writestr("Version", "1.32".encode("utf-16le"))
    z.writestr("DiagramLayout", json.dumps(diagram_layout).encode("utf-16le"))
    z.writestr("Settings", json.dumps({"Version": 4, "ReportSettings": {}, "QueriesSettings": {"TypeDetectionEnabled": True, "RelationshipImportEnabled": True, "Version": "2.158.928.0"}}).encode("utf-16le"))
    z.writestr("Metadata", json.dumps({"Version": 5, "AutoCreatedRelationships": [], "CreatedFrom": "Cloud", "CreatedFromRelease": "2026.09"}).encode("utf-16le"))
    z.writestr("DataModelSchema", json.dumps(tmsl_model).encode("utf-16le"))
    
    # Add files from definition/ and StaticResources/ ONLY (excluding definition.pbir)
    for root, _, files in os.walk(REPORT_DEF_DIR):
        for file in files:
            full_p = os.path.join(root, file)
            rel_p = os.path.relpath(full_p, REPORT_DEF_DIR)
            arc_name = "Report/definition/" + rel_p.replace("\\", "/")
            z.write(full_p, arc_name)
            
    for root, _, files in os.walk(static_dest):
        for file in files:
            full_p = os.path.join(root, file)
            rel_p = os.path.relpath(full_p, static_dest)
            arc_name = "Report/StaticResources/" + rel_p.replace("\\", "/")
            z.write(full_p, arc_name)

print(f"PBIT generated: {pbit_path}")

# Copy to Mauricio's Desktop directly!
desktop_pbit = os.path.join(DESKTOP_DIR, "Workshop3_Happiness_Dashboard.pbit")
shutil.copy2(pbit_path, desktop_pbit)
print(f"Copied to Desktop: {desktop_pbit}")

# Also copy launcher to Desktop
desktop_bat = os.path.join(DESKTOP_DIR, "ABRIR_DASHBOARD_POWER_BI.bat")
with open(desktop_bat, "w", encoding="utf-8") as f:
    f.write(r'''@echo off
chcp 65001 >nul
echo ========================================================
echo   WORKSHOP 3 - STREAMING ETL & ML (POWER BI DASHBOARD)
echo   Autor: Mauricio Taborda
echo ========================================================
echo.
echo [1/2] Verificando contenedores de base de datos...
docker start happiness_postgres happiness_zookeeper happiness_kafka >nul 2>&1

echo [2/2] Abriendo Dashboard en Power BI Desktop...
set PBIT_PATH=%~dp0Workshop3_Happiness_Dashboard.pbit
explorer.exe "%PBIT_PATH%"

echo.
echo ========================================================
echo   Dashboard lanzado. En Power BI haz clic en:
echo   'Actualizar' o 'Actualizar ahora' para refrescar datos.
echo ========================================================
timeout /t 5
''')
print(f"Launcher copied to Desktop: {desktop_bat}")
