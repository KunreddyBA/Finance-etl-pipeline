# scripts/etl_pipeline_pandas.py

import pandas as pd
import os
import re
from datetime import datetime

def run_etl():
    """
    Main ETL function to process LendingClub loan data using pandas.
    """
    # Define file paths
    project_root = os.path.dirname(os.path.dirname(__file__))
    input_path = os.path.join(project_root, 'data/raw/loans.csv')
    output_path = os.path.join(project_root, 'data/processed/loans_processed.parquet')

    print(f"Reading raw data from {input_path}...")
    df = pd.read_csv(input_path)

    # --- Transformation ---
    print("Starting data transformation...")

    # 1. Select a subset of columns
    columns_to_keep = [
        "loan_amnt", "term", "int_rate", "grade", "emp_length", "home_ownership",
        "annual_inc", "verification_status", "issue_d", "loan_status",
        "purpose", "addr_state", "dti"
    ]
    df_selected = df[columns_to_keep].copy()

    # 2. Clean and cast data types
    print("Cleaning and transforming data...")
    
    # Convert numeric columns
    df_selected['loan_amnt'] = pd.to_numeric(df_selected['loan_amnt'], errors='coerce')
    df_selected['annual_inc'] = pd.to_numeric(df_selected['annual_inc'], errors='coerce')
    df_selected['dti'] = pd.to_numeric(df_selected['dti'], errors='coerce')
    
    # Clean interest rate (remove % and convert to float)
    df_selected['int_rate'] = df_selected['int_rate'].str.replace('%', '').astype(float)
    
    # Convert issue date
    df_selected['issue_d'] = pd.to_datetime(df_selected['issue_d'], format='%b-%Y', errors='coerce')
    
    # Clean term column (extract months)
    df_selected['term_months'] = df_selected['term'].str.extract(r'(\d+)').astype(int)
    
    # Clean employment length
    def clean_emp_length(emp_length):
        if pd.isna(emp_length):
            return 0
        if emp_length == "< 1 year":
            return 0
        if emp_length == "10+ years":
            return 10
        # Extract number from "X years" format
        match = re.search(r'(\d+)', str(emp_length))
        if match:
            return int(match.group(1))
        return 0
    
    df_selected['emp_years'] = df_selected['emp_length'].apply(clean_emp_length)
    
    # Drop original columns that have been cleaned
    df_final = df_selected.drop(['term', 'emp_length'], axis=1)
    
    # Filter out rows where loan_status is null
    df_final = df_final.dropna(subset=['loan_status'])

    # --- Load ---
    print(f"Writing processed data to {output_path}...")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save as parquet
    df_final.to_parquet(output_path, index=False)

    print("ETL process completed successfully!")
    print(f"Processed {len(df_final)} records")
    print(f"Output saved to: {output_path}")
    
    # Show sample of processed data
    print("\nSample of processed data:")
    print(df_final.head())
    
    return df_final

if __name__ == "__main__":
    run_etl()
