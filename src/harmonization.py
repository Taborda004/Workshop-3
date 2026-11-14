"""
Data Harmonization and Preprocessing Module
Course: ETL (G01) - Workshop 3: Streaming ETL with Apache Kafka and Machine Learning
Author: Mauricio Taborda

This module handles schema harmonization, cleaning, and region mapping
for World Happiness Report datasets (2015 - 2019).
"""

import os
import pandas as pd
import numpy as np


# Column mapping per year to target unified schema
YEAR_COLUMN_MAPPINGS = {
    2015: {
        'Country': 'country',
        'Happiness Score': 'actual_happiness_score',
        'Economy (GDP per Capita)': 'gdp',
        'Family': 'family',
        'Health (Life Expectancy)': 'health',
        'Freedom': 'freedom',
        'Generosity': 'generosity',
        'Trust (Government Corruption)': 'corruption',
        'Region': 'region'
    },
    2016: {
        'Country': 'country',
        'Happiness Score': 'actual_happiness_score',
        'Economy (GDP per Capita)': 'gdp',
        'Family': 'family',
        'Health (Life Expectancy)': 'health',
        'Freedom': 'freedom',
        'Generosity': 'generosity',
        'Trust (Government Corruption)': 'corruption',
        'Region': 'region'
    },
    2017: {
        'Country': 'country',
        'Happiness.Score': 'actual_happiness_score',
        'Economy..GDP.per.Capita.': 'gdp',
        'Family': 'family',
        'Health..Life.Expectancy.': 'health',
        'Freedom': 'freedom',
        'Generosity': 'generosity',
        'Trust..Government.Corruption.': 'corruption'
    },
    2018: {
        'Country or region': 'country',
        'Score': 'actual_happiness_score',
        'GDP per capita': 'gdp',
        'Social support': 'family',
        'Healthy life expectancy': 'health',
        'Freedom to make life choices': 'freedom',
        'Generosity': 'generosity',
        'Perceptions of corruption': 'corruption'
    },
    2019: {
        'Country or region': 'country',
        'Score': 'actual_happiness_score',
        'GDP per capita': 'gdp',
        'Social support': 'family',
        'Healthy life expectancy': 'health',
        'Freedom to make life choices': 'freedom',
        'Generosity': 'generosity',
        'Perceptions of corruption': 'corruption'
    }
}

# Country name normalization dictionary for longitudinal consistency
COUNTRY_NAME_STANDARDIZATION = {
    'United States': 'United States',
    'Hong Kong S.A.R., China': 'Hong Kong',
    'Hong Kong': 'Hong Kong',
    'Taiwan Province of China': 'Taiwan',
    'Taiwan': 'Taiwan',
    'Trinidad & Tobago': 'Trinidad and Tobago',
    'Trinidad and Tobago': 'Trinidad and Tobago',
    'North Macedonia': 'Macedonia',
    'Macedonia': 'Macedonia',
    'Northern Cyprus': 'North Cyprus',
    'Somaliland Region': 'Somalia',
    'Somaliland region': 'Somalia'
}

# Manual region fallback for countries not present in 2015/2016
REGION_FALLBACKS = {
    'Somalia': 'Sub-Saharan Africa',
    'Belize': 'Latin America and Caribbean',
    'South Sudan': 'Sub-Saharan Africa',
    'Namibia': 'Sub-Saharan Africa',
    'Hong Kong': 'Eastern Asia',
    'Taiwan': 'Eastern Asia',
    'Trinidad and Tobago': 'Latin America and Caribbean',
    'North Cyprus': 'Western Europe',
    'Macedonia': 'Central and Eastern Europe',
    'Gambia': 'Sub-Saharan Africa',
    'Puerto Rico': 'Latin America and Caribbean'
}


def get_project_root():
    """Returns absolute path to the project root directory."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(current_dir, '..'))


def load_and_harmonize_data(raw_dir=None) -> pd.DataFrame:
    """
    Reads all raw yearly CSVs (2015-2019), standardizes column names,
    normalizes country names, enriches missing regions, and handles missing values.
    """
    if raw_dir is None:
        raw_dir = os.path.join(get_project_root(), 'data', 'raw')
    dfs = []
    base_regions = {}

    # Extract base regions from 2015 and 2016
    for yr in [2015, 2016]:
        fp = os.path.join(raw_dir, f'{yr}.csv')
        if os.path.exists(fp):
            tmp = pd.read_csv(fp)
            c_col = 'Country'
            r_col = 'Region'
            if c_col in tmp.columns and r_col in tmp.columns:
                for _, row in tmp[[c_col, r_col]].dropna().iterrows():
                    c_clean = COUNTRY_NAME_STANDARDIZATION.get(row[c_col].strip(), row[c_col].strip())
                    base_regions[c_clean] = row[r_col].strip()

    base_regions.update(REGION_FALLBACKS)

    for year in sorted(YEAR_COLUMN_MAPPINGS.keys()):
        file_path = os.path.join(raw_dir, f'{year}.csv')
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Missing expected raw data file: {file_path}")

        raw_df = pd.read_csv(file_path)
        col_map = YEAR_COLUMN_MAPPINGS[year]
        
        # Select existing mapped columns
        mapped_cols = [c for c in col_map.keys() if c in raw_df.columns]
        subset_df = raw_df[mapped_cols].copy()
        
        # Invert dictionary for renaming to snake_case target schema
        rename_dict = {orig: target for orig, target in col_map.items() if orig in subset_df.columns}
        subset_df.rename(columns=rename_dict, inplace=True)
        
        subset_df['year'] = int(year)
        
        # Clean country names
        subset_df['country'] = subset_df['country'].astype(str).str.strip()
        subset_df['country'] = subset_df['country'].map(lambda c: COUNTRY_NAME_STANDARDIZATION.get(c, c))
        
        # Assign region
        if 'region' not in subset_df.columns:
            subset_df['region'] = subset_df['country'].map(lambda c: base_regions.get(c, 'Other'))
        else:
            subset_df['region'] = subset_df.apply(
                lambda row: base_regions.get(row['country'], row['region']), axis=1
            )
            
        dfs.append(subset_df)

    unified_df = pd.concat(dfs, ignore_index=True)
    
    # Harmonize numeric column types
    numeric_cols = ['actual_happiness_score', 'gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
    for col in numeric_cols:
        unified_df[col] = pd.to_numeric(unified_df[col], errors='coerce')
        
    # Handle single missing value in UAE 2018 corruption by imputing median of UAE
    uae_corruption_median = unified_df[unified_df['country'] == 'United Arab Emirates']['corruption'].median()
    unified_df['corruption'] = unified_df['corruption'].fillna(uae_corruption_median)
    
    # Enforce column order
    ordered_cols = ['country', 'region', 'year', 'actual_happiness_score', 'gdp', 'family', 'health', 'freedom', 'generosity', 'corruption']
    unified_df = unified_df[ordered_cols]
    
    return unified_df


def export_processed_datasets(output_processed_dir=None, output_streaming_dir=None):
    """
    Exports the unified dataset for offline ML and creates a streaming dataset partition.
    """
    root = get_project_root()
    if output_processed_dir is None:
        output_processed_dir = os.path.join(root, 'data', 'processed')
    if output_streaming_dir is None:
        output_streaming_dir = os.path.join(root, 'data', 'streaming')
        
    os.makedirs(output_processed_dir, exist_ok=True)
    os.makedirs(output_streaming_dir, exist_ok=True)
    
    df = load_and_harmonize_data()
    
    # Save master processed dataset
    processed_path = os.path.join(output_processed_dir, 'happiness_unified.csv')
    df.to_csv(processed_path, index=False)
    print(f"Exported unified dataset to {processed_path} (Shape: {df.shape})")
    
    # Save streaming simulation dataset
    # Sort chronologically and by country to simulate realistic event flow
    stream_df = df.sort_values(by=['year', 'country']).reset_index(drop=True)
    streaming_path = os.path.join(output_streaming_dir, 'happiness_stream_source.csv')
    stream_df.to_csv(streaming_path, index=False)
    print(f"Exported streaming source dataset to {streaming_path} (Shape: {stream_df.shape})")
    
    return df


if __name__ == '__main__':
    export_processed_datasets()
