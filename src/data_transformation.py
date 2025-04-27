import pandas as pd
import os
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import numpy as np


PREPROCESSED_DIR = Path('../preprocessed')
RESAMPLED_DIR = Path('../resampled')
NORMALIZED_DIR = Path('../normalized')
STANDARDIZED_DIR = Path('../standardized')

# Create directories
RESAMPLED_DIR.mkdir(exist_ok=True)
NORMALIZED_DIR.mkdir(exist_ok=True)
STANDARDIZED_DIR.mkdir(exist_ok=True)

def load_and_transform_date(file_path):
    df = pd.read_csv(file_path, parse_dates=['Date'],
                    date_format='%Y-%m-%d')
    
    df['Date'] = pd.to_datetime(df['Date'].str.replace(' UTC+0', ''))

    df = df.set_index('Date').sort_index()
    return df

def reindex_data_ffill(df):
    full_idx = pd.date_range(df.index.min(), df.index.max(), freq='D')
    df = df.reindex(full_idx)
    df = df.ffill()
    return df

def normalize_data(df):
    scaler = MinMaxScaler()
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    return df

def standardize_data(df):
    scaler = StandardScaler()
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
    return df

def add_analysis_data(df, window_sizes=[7, 14, 30, 90]):
    # decided to use 1-week, 2-week, 1-month, 3-month rolling periods

    df["Return"] = df["Price"].pct_change()
    df["Log_Return"] = np.log(df["Price"]).diff()

    for w in window_sizes:
        # Simple Moving Average of price
        df[f'MA_{w}'] = df['Price'].rolling(window=w, min_periods=1).mean()
        
        # Rolling volatility of simple returns
        df[f'Vol_{w}'] = df['Return'].rolling(window=w, min_periods=1).std()

    return df


for file_path in PREPROCESSED_DIR.glob('*.csv'):
    # Update the original dataset with transformed dates
    df = load_and_transform_date(file_path)
    df = add_analysis_data(df)

    df.to_csv(file_path)
    
    # Create resampled version with forward fill and add analysis data
    df_resampled = reindex_data_ffill(df)
    df_resampled = add_analysis_data(df_resampled)

    # Save resampled version
    output_path = RESAMPLED_DIR / file_path.name
    df_resampled.to_csv(output_path, index_label='Date')
    
    # Create and save normalized version
    df_normalized = normalize_data(df_resampled.copy())
    normalized_path = NORMALIZED_DIR / file_path.name
    df_normalized.to_csv(normalized_path, index_label='Date')
    
    # Create and save standardized version  
    df_standardized = standardize_data(df_resampled.copy())
    standardized_path = STANDARDIZED_DIR / file_path.name
    df_standardized.to_csv(standardized_path, index_label='Date')


# Appendix B visualization
print("\nPreprocessed BTC data:")
print(pd.read_csv(PREPROCESSED_DIR / 'BTC.csv').head())

print("\nResampled BTC data:")
print(pd.read_csv(RESAMPLED_DIR / 'BTC.csv').head())

print("\nNormalized BTC data:")
print(pd.read_csv(NORMALIZED_DIR / 'BTC.csv').head())

print("\nStandardized BTC data:")
print(pd.read_csv(STANDARDIZED_DIR / 'BTC.csv').head())
