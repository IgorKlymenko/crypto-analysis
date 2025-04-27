import pandas as pd
from pathlib import Path

class DataCleaner:
    def __init__(self, currency_name):
        self.df = pd.read_csv(f'../data/{currency_name}.csv')
        self.currency_name = currency_name
        self.cleaning_report = []

    def drop_duplicates(self):
        initial_rows = len(self.df)
        self.df = self.df.drop_duplicates()
        dropped_rows = initial_rows - len(self.df)
        if dropped_rows > 0:
            self.cleaning_report.append(f"Dropped {dropped_rows} duplicate rows")

    def inspect_file(self):
        print(f"Inspecting {self.currency_name}")
        print("--------------------------------")
        print("General info:")
        print(self.df.shape)
        print(self.df.info())
        print(self.df.describe())
        
        print("--------------------------------")
        print("Missing values:")
        missing = self.df.isna().sum()
        missing_pct = (missing / len(self.df) * 100).round(2)
        missing_summary = pd.concat([missing, missing_pct], axis=1, keys=['count', 'pct'])
        print(missing_summary)
        
        # Record missing values info into the cleaning report
        for col in missing.index:
            if missing[col] > 0:
                self.cleaning_report.append(f"Found {missing[col]} missing values ({missing_pct[col]}%) in column {col}")
        
        rows_with_missing = self.df.isna().any(axis=1).sum()
        total_missing_pct = (rows_with_missing / len(self.df) * 100).round(2)
        print(f"\nTotal rows with any missing values: {rows_with_missing} ({total_missing_pct}%)")
        
        print("--------------------------------")
        print("Duplicates:")
        duplicates = self.df.duplicated().sum()
        print(duplicates)
        if duplicates > 0:
            self.cleaning_report.append(f"Found {duplicates} duplicate rows")
            
        print("--------------------------------")

        # Here we check for negative values in each of the columns
        invalid_price = self.df[self.df['Price'] < 0]
        if not invalid_price.empty:
            print("Price ≤ 0:", invalid_price)
            self.cleaning_report.append(f"Found {len(invalid_price)} rows with invalid Price values (≤ 0)")
            
        invalid_vol = self.df[self.df['Volume'] < 0]
        if not invalid_vol.empty:
            print("Volume < 0:", invalid_vol)
            self.cleaning_report.append(f"Found {len(invalid_vol)} rows with invalid Volume values (< 0)")
            
        invalid_market_cap = self.df[self.df['Market_cap'] < 0]
        if not invalid_market_cap.empty:
            print("Market_cap < 0:", invalid_market_cap)
            self.cleaning_report.append(f"Found {len(invalid_market_cap)} rows with invalid Market_cap values (< 0)")

        if invalid_price.empty and invalid_vol.empty and invalid_market_cap.empty:
            print("No invalid values found")
            self.cleaning_report.append("No invalid values found in Price, Volume or Market_cap")
        
        print("--------------------------------")
        print(self.df.head(10))
        print("--------------------------------")

    def drop_missing(self):
        # We consider both NaN and 0 values as missing
        missing_mask = self.df.isna() | (self.df == 0)
        rows_with_missing = missing_mask.any(axis=1).sum()
        total_missing_pct = (rows_with_missing / len(self.df) * 100).round(2)
        
        if total_missing_pct < 5:
            print(f"Dropping {rows_with_missing} rows with missing values (NaN or 0) ({total_missing_pct}%)")
            initial_rows = len(self.df)
            self.df = self.df[(~missing_mask).all(axis=1)]
            dropped_rows = initial_rows - len(self.df)
            self.cleaning_report.append(f"Dropped {dropped_rows} rows with missing values (NaN or 0) ({total_missing_pct}%)")
        else:
            # After inspection of datasets, we can see that almost no price values are missing or zero
            # So we create two datasets, one with only price values and one with all values
            print(f"Creating two datasets as missing values represent {total_missing_pct}% of total data")

            price_missing = self.df['Price'].isna() | (self.df['Price'] == 0)
            timeseries_df = self.df[['Date', 'Price']][~price_missing]
            timeseries_df.to_csv(f'../preprocessed/{self.currency_name}_price_only.csv', index=False)

            full_df = self.df[(~missing_mask).all(axis=1)]
            self.df = full_df
            self.cleaning_report.append(f"Created price-only dataset with {len(timeseries_df)} rows due to high missing values (NaN or 0) ({total_missing_pct}%)")
            self.cleaning_report.append(f"Created full dataset with {len(full_df)} rows after dropping missing values (NaN or 0)")

    def save_file(self):
        self.df.to_csv(f'../preprocessed/{self.currency_name}.csv', index=False)
        
        # Save the cleaning report
        report_text = f"\nCleaning Report for {self.currency_name}\n"
        report_text += "==================================================" + "\n"
        report_text += "\n".join(self.cleaning_report)
        report_text += "\n\n"
        
        with open('../initial_cleaning_report.txt', 'a') as f:
            f.write(report_text)

def process_all_files():
    # Create/clear the report file
    with open('../initial_cleaning_report.txt', 'w') as f:
        f.write("Initial Data Cleaning Report\n")
        f.write("==========================\n\n")
    
    # Get all CSV files in data directory
    data_dir = Path('../data')
    csv_files = list(data_dir.glob('*.csv'))
    
    for csv_file in csv_files:
        crypto_name = csv_file.stem
        print(f"\nProcessing {crypto_name}...")
        
        cleaner = DataCleaner(crypto_name)
        cleaner.drop_duplicates()
        cleaner.inspect_file()
        cleaner.drop_missing()
        cleaner.save_file()

if __name__ == "__main__":
    process_all_files()

