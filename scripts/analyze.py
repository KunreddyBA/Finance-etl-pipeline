# scripts/analyze.py

import duckdb
import os
import pandas as pd

def analyze_data():
    """
    Connects to DuckDB, queries the processed data, and prints results.
    """
    # Define file paths
    project_root = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(project_root, 'warehouse/lendingclub.db')
    parquet_path = os.path.join(project_root, 'data/processed/loans_processed.parquet')

    # Connect to DuckDB (it will create the file if it doesn't exist)
    con = duckdb.connect(database=db_path, read_only=False)

    print("Querying processed data with DuckDB...")

    # SQL query to perform analysis
    query = f"""
    SELECT
        grade,
        COUNT(*) AS num_loans,
        AVG(loan_amnt) AS avg_loan_amount,
        AVG(int_rate) AS avg_interest_rate
    FROM '{parquet_path}'
    GROUP BY grade
    ORDER BY grade;
    """

    # Execute and fetch results as a Pandas DataFrame
    results_df = con.execute(query).fetchdf()

    print("Analysis Results by Loan Grade:")
    print(results_df)

    # Export analysis results to CSV
    analysis_csv_path = os.path.join(project_root, 'data/processed/analysis_results.csv')
    results_df.to_csv(analysis_csv_path, index=False)
    print(f"\n📊 Analysis results exported to: {analysis_csv_path}")

    con.close()
    
    return results_df

def export_full_data_to_csv():
    """
    Export the full processed dataset to CSV.
    """
    # Define file paths
    project_root = os.path.dirname(os.path.dirname(__file__))
    parquet_path = os.path.join(project_root, 'data/processed/loans_processed.parquet')
    csv_output_path = os.path.join(project_root, 'data/processed/loans_processed.csv')
    
    print(f"Exporting full dataset to CSV...")
    
    try:
        # Connect to DuckDB
        con = duckdb.connect(':memory:')
        
        # Read all data
        df = con.execute(f"SELECT * FROM '{parquet_path}'").fetchdf()
        
        # Export to CSV
        df.to_csv(csv_output_path, index=False)
        
        print(f"✅ Successfully exported {len(df)} records to CSV")
        print(f"📁 CSV file saved to: {csv_output_path}")
        
        con.close()
        return True
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {e}")
        return False

if __name__ == "__main__":
    print("📊 LendingClub Data Analysis & Export")
    print("=" * 50)
    
    # Run analysis
    results = analyze_data()
    
    # Export full data to CSV
    print("\n" + "=" * 50)
    print("📁 Exporting data to CSV...")
    export_full_data_to_csv()
    
    print("\n" + "=" * 50)
    print("🎉 Analysis and export completed!")
    print("\n📋 Generated files:")
    print("- Full dataset: data/processed/loans_processed.csv")
    print("- Analysis results: data/processed/analysis_results.csv")
    print("- Original Parquet: data/processed/loans_processed.parquet")
