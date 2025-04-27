import pandas as pd
from pathlib import Path
import os
import matplotlib.pyplot as plt
def generate_statistical_reports(preprocessed_dir, reports_dir):
    reports_dir.mkdir(exist_ok=True)
    csv_files = list(preprocessed_dir.glob('*.csv'))

    report_path_basic = reports_dir / "basic_statistical_analysis.txt"
    report_path_extended = reports_dir / "extended_statistical_analysis.txt"

    # Clean up existing report files if they exist
    if report_path_basic.exists():
        report_path_basic.unlink()
        
    if report_path_extended.exists():
        report_path_extended.unlink()

    with open(report_path_basic, 'a') as f:
        f.write("Basic Statistical Analysis\n")

    with open(report_path_extended, 'a') as f:
        f.write("Extended Statistical Analysis\n")

    for file_path in csv_files:
        # Read the data
        df = pd.read_csv(file_path)
        df.set_index("Date", inplace=True)
        
        basic_report = ["=" * 50 + "\n"]
        extended_report = ["=" * 50 + "\n"]
        
        ## BASIC REPORT

        if 'Volume' in df.columns and 'Market_cap' in df.columns:
            columns = ['Price', 'Volume', 'Market_cap']
        else:
            columns = ['Price']

        basic_stats = df[columns].agg(['mean', 'median', 'std'])
        basic_report.append(f"Basic Statistics for {file_path.name}:\n{basic_stats.to_string()}\n")

        if 'Volume' in df.columns and 'Market_cap' in df.columns:
            basic_corr = df[columns].corr()
            basic_report.append(f"Basic Correlation Matrix:\n{basic_corr.to_string()}\n")

        ## EXTENDED REPORT
        extended_stats = df.describe()
        extended_report.append(f"Extended Statistics for {file_path.name}:\n{extended_stats.to_string()}\n")

        extended_corr = df.corr()
        extended_report.append(f"Full Correlation Matrix:\n{extended_corr.to_string()}\n")

        # Save the basic statistical analysis report
        with open(report_path_basic, 'a') as f:
            f.write("\n".join(basic_report))
            f.write("\n\n")

        # Save the extended statistical analysis report
        with open(report_path_extended, 'a') as f:
            f.write("\n".join(extended_report))
            f.write("\n\n")


def plot_moving_averages(title):
    df = pd.read_csv(f'../preprocessed/{title}.csv')
    df.set_index("Date", inplace=True)

    plt.figure(figsize=(12,6))
    plt.plot(df.index, df['Price'], label='Price', alpha=0.6)
    plt.plot(df.index, df['MA_7'], label='MA 7-day', linewidth=2)
    plt.plot(df.index, df['MA_30'], label='MA 30-day', linewidth=2)
    plt.plot(df.index, df['MA_90'], label='MA 90-day', linewidth=2)
    plt.title(f"Price with 7-day, 30-day, 90-day Moving Averages for {title}")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.show()


reports_dir = Path('../reports')
preprocessed_dir = Path('../preprocessed')
# generate_statistical_reports(preprocessed_dir, reports_dir)

plot_moving_averages('Lido')
