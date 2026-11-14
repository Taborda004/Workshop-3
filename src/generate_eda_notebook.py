"""
Generator script for notebooks/eda.ipynb
"""

import nbformat as nbf
import os

def create_eda_notebook():
    nb = nbf.v4.new_notebook()
    cells = []

    # Cell 1: Markdown Title & Context
    cells.append(nbf.v4.new_markdown_cell("""# Workshop-3: Exploratory Data Analysis & Schema Harmonization
## Academic Course: ETL (G01) - Data Engineering & AI
### Objective
Inspect raw historical World Happiness datasets (2015-2019), identify schema differences, missing values, duplicates, and outliers, and build a unified analytical schema for downstream ML and streaming.
"""))

    # Cell 2: Imports
    cells.append(nbf.v4.new_code_cell("""import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['font.size'] = 11
print('Environment and libraries ready.')
"""))

    # Cell 3: Loading raw data
    cells.append(nbf.v4.new_markdown_cell("""## 1. Inspecting Raw Datasets (2015 - 2019)
We load the five CSV files located in `../data/raw/` and inspect their shapes, column structures, and data types.
"""))

    cells.append(nbf.v4.new_code_cell("""raw_data_dir = '../data/raw'
years = [2015, 2016, 2017, 2018, 2019]
raw_dfs = {}

for y in years:
    path = os.path.join(raw_data_dir, f'{y}.csv')
    raw_dfs[y] = pd.read_csv(path)
    print(f'Year {y}: Shape = {raw_dfs[y].shape}, Columns = {len(raw_dfs[y].columns)}')
"""))

    # Cell 4: Schema Comparison Table
    cells.append(nbf.v4.new_markdown_cell("""## 2. Schema Differences & Column Name Inconsistencies
Notice how column names change across years. In 2015 and 2016 names contain spaces and parentheses. In 2017 names contain dots. In 2018 and 2019 names are shorter, and column count dropped from 12-13 to 9.
"""))

    cells.append(nbf.v4.new_code_cell("""schema_summary = []
for y in years:
    cols = raw_dfs[y].columns.tolist()
    schema_summary.append({
        'Year': y,
        'Num_Rows': raw_dfs[y].shape[0],
        'Num_Cols': raw_dfs[y].shape[1],
        'Columns': cols
    })

for item in schema_summary:
    print(f"=== YEAR {item['Year']} ===")
    print(item['Columns'])
    print()
"""))

    # Cell 5: Missing Values & Duplicates Audit
    cells.append(nbf.v4.new_markdown_cell("""## 3. Data Quality Audit: Missing Values & Duplicates
We check for null values and duplicated rows in all datasets.
"""))

    cells.append(nbf.v4.new_code_cell("""print('--- DUPLICATE ROWS CHECK ---')
for y in years:
    dups = raw_dfs[y].duplicated().sum()
    print(f'{y}: {dups} duplicated rows.')

print('\\n--- MISSING VALUES (NaN) CHECK ---')
for y in years:
    nulls = raw_dfs[y].isnull().sum()
    null_cols = nulls[nulls > 0]
    if len(null_cols) > 0:
        print(f'Year {y} has missing values:\\n{null_cols}')
    else:
        print(f'Year {y}: 0 missing values.')
"""))

    # Cell 6: Detailed check on UAE missing corruption
    cells.append(nbf.v4.new_code_cell("""# Inspect the single missing value in 2018
df_2018 = raw_dfs[2018]
missing_uae = df_2018[df_2018['Perceptions of corruption'].isnull()]
missing_uae[['Overall rank', 'Country or region', 'Score', 'Perceptions of corruption']]
"""))

    # Cell 7: Data Quality Observations
    cells.append(nbf.v4.new_markdown_cell("""## 4. Data Quality Observations & Target Leakage Prevention

### Observations:
1. **Schema Evolution:**
   - 2015-2016 include `Region`, `Standard Error` / `Confidence Interval`, and `Dystopia Residual`.
   - 2017 switched formatting to dot notation (e.g., `Economy..GDP.per.Capita.`, `Happiness.Score`).
   - 2018-2019 dropped `Region` and error metrics, renamed `Country` to `Country or region`, and `Family` to `Social support`.
2. **Missing Values:**
   - Only 1 missing value exists across all 782 records: `Perceptions of corruption` for `United Arab Emirates` in 2018.
3. **Duplicates:** Zero duplicated rows across all 5 years.
4. **Target Leakage Risks (Critical):**
   - `Happiness Rank` / `Overall rank`: Directly computed by ranking `Happiness Score`. Using it as an input feature would be pure target leakage.
   - `Dystopia Residual`: Component mathematically tied to the target sum ($Score = Sum(factors) + Residual$).
   - `Standard Error` / `Confidence Intervals`: Variance estimates directly measuring the target score's distribution.
   - **Conclusion:** Features must be restricted to the 6 core explanatory metrics: `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`.
"""))

    # Cell 8: Unified Schema Proposal
    cells.append(nbf.v4.new_markdown_cell("""## 5. Unified Schema Proposal
We define an analytical `snake_case` schema matching the Kafka Event Contract:

| Unified Column | Type | Source 2015-2016 | Source 2017 | Source 2018-2019 | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `country` | string | `Country` | `Country` | `Country or region` | Standardized country name |
| `region` | string | `Region` | Inferred / Reference | Inferred / Reference | Geographic region |
| `year` | integer | Generated (2015/16) | Generated (2017) | Generated (2018/19) | Observation year |
| `actual_happiness_score` | float | `Happiness Score` | `Happiness.Score` | `Score` | Target variable (evaluation only) |
| `gdp` | float | `Economy (GDP per Capita)` | `Economy..GDP.per.Capita.` | `GDP per capita` | Feature 1: Economic output |
| `family` | float | `Family` | `Family` | `Social support` | Feature 2: Social/family support |
| `health` | float | `Health (Life Expectancy)` | `Health..Life.Expectancy.` | `Healthy life expectancy` | Feature 3: Life expectancy |
| `freedom` | float | `Freedom` | `Freedom` | `Freedom to make life choices` | Feature 4: Freedom of choice |
| `generosity` | float | `Generosity` | `Generosity` | `Generosity` | Feature 5: Generosity score |
| `corruption` | float | `Trust (Government Corruption)`| `Trust..Government.Corruption.`| `Perceptions of corruption` | Feature 6: Absence of corruption |
"""))

    # Cell 9: Harmonization implementation
    cells.append(nbf.v4.new_markdown_cell("""## 6. Implementation of Cleaning and Harmonization
We execute the standardized transformation and export the unified dataset to `../data/processed/happiness_unified.csv`.
"""))

    cells.append(nbf.v4.new_code_cell("""sys.path.append('..')
from src.harmonization import load_and_harmonize_data, export_processed_datasets

unified_df = export_processed_datasets(
    output_processed_dir='../data/processed',
    output_streaming_dir='../data/streaming'
)

print('Harmonized Dataset Head:')
unified_df.head()
"""))

    # Cell 10: Statistical validation & distributions
    cells.append(nbf.v4.new_markdown_cell("""## 7. Descriptive Statistics and Visual Outlier Analysis
"""))

    cells.append(nbf.v4.new_code_cell("""print('Unified Dataset Descriptive Statistics:')
print(unified_df.describe().round(4))
"""))

    # Cell 11: Correlation Heatmap
    cells.append(nbf.v4.new_code_cell("""fig, ax = plt.subplots(figsize=(10, 8))
numeric_cols = ['actual_happiness_score', 'gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
corr = unified_df[numeric_cols].corr()
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.3f', cbar=True, ax=ax, linewidths=0.5)
ax.set_title('Correlation Heatmap: Features vs. Actual Happiness Score', fontsize=14, fontweight='bold')
plt.tight_layout()
os.makedirs('../dashboards', exist_ok=True)
plt.savefig('../dashboards/correlation_heatmap.png', dpi=300)
plt.show()
"""))

    # Cell 12: Boxplot for Outliers
    cells.append(nbf.v4.new_code_cell("""fig, ax = plt.subplots(figsize=(12, 6))
sns.boxplot(data=unified_df[numeric_cols[1:]], ax=ax, palette='Set2')
ax.set_title('Distribution & Outlier Inspection of Explanatory Features', fontsize=14, fontweight='bold')
ax.set_ylabel('Indicator Value')
plt.xticks(rotation=25)
plt.tight_layout()
plt.savefig('../dashboards/features_boxplot.png', dpi=300)
plt.show()
"""))

    # Cell 13: Summary markdown
    cells.append(nbf.v4.new_markdown_cell("""## 8. Conclusion of Step 1 & Step 2
- Total observations harmonized: **782 records** across 5 years.
- Zero missing values remaining after justified median imputation of UAE 2018 corruption.
- Clear, leakage-free feature subset identified: `['gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']`.
- Unified dataset exported to `../data/processed/happiness_unified.csv`.
"""))

    nb.cells = cells
    os.makedirs('notebooks', exist_ok=True)
    nb_path = os.path.join('notebooks', 'eda.ipynb')
    with open(nb_path, 'w', encoding='utf-8') as f:
        nbf.write(nb, f)
    print(f"Generated {nb_path}")

if __name__ == '__main__':
    create_eda_notebook()
