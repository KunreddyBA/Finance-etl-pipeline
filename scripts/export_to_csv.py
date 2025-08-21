# scripts/export_to_csv.py

import pandas as pd
import os
import duckdb

def export_to_csv():
    """
    Export the processed Parquet data to CSV format.
    """
    # Define file paths
    project_root = os.path.dirname(os.path.dirname(__file__))
    parquet_path = os.path.join(project_root, 'data/processed/loans_processed.parquet')
    csv_output_path = os.path.join(project_root, 'data/processed/loans_processed.csv')
    
    print(f"Reading processed data from {parquet_path}...")
    
    # Method 1: Using DuckDB to read Parquet and export to CSV
    try:
        # Connect to DuckDB
        con = duckdb.connect(':memory:')
        
        # Read Parquet data
        df = con.execute(f"SELECT * FROM '{parquet_path}'").fetchdf()
        
        # Export to CSV
        df.to_csv(csv_output_path, index=False)
        
        print(f"✅ Successfully exported {len(df)} records to CSV")
        print(f"📁 CSV file saved to: {csv_output_path}")
        
        # Show sample data
        print("\n📊 Sample of exported data:")
        print(df.head())
        
        # Show data info
        print(f"\n📈 Data Summary:")
        print(f"- Total records: {len(df)}")
        print(f"- Columns: {list(df.columns)}")
        print(f"- File size: {os.path.getsize(csv_output_path) / 1024:.1f} KB")
        
        con.close()
        
    except Exception as e:
        print(f"❌ Error using DuckDB: {e}")
        print("🔄 Trying alternative method with pandas...")
        
        # Method 2: Using pandas directly
        try:
            df = pd.read_parquet(parquet_path)
            df.to_csv(csv_output_path, index=False)
            
            print(f"✅ Successfully exported {len(df)} records to CSV")
            print(f"📁 CSV file saved to: {csv_output_path}")
            
            # Show sample data
            print("\n📊 Sample of exported data:")
            print(df.head())
            
        except Exception as e2:
            print(f"❌ Error using pandas: {e2}")
            return False
    
    return True

def export_sample_data():
    """
    Export a sample of the data for quick viewing.
    """
    # Define file paths
    project_root = os.path.dirname(os.path.dirname(__file__))
    parquet_path = os.path.join(project_root, 'data/processed/loans_processed.parquet')
    sample_csv_path = os.path.join(project_root, 'data/processed/loans_sample.csv')
    
    try:
        # Connect to DuckDB
        con = duckdb.connect(':memory:')
        
        # Read sample data (first 100 records)
        df_sample = con.execute(f"SELECT * FROM '{parquet_path}' LIMIT 100").fetchdf()
        
        # Export to CSV
        df_sample.to_csv(sample_csv_path, index=False)
        
        print(f"✅ Successfully exported sample of {len(df_sample)} records to CSV")
        print(f"📁 Sample CSV file saved to: {sample_csv_path}")
        
        con.close()
        
    except Exception as e:
        print(f"❌ Error exporting sample: {e}")

if __name__ == "__main__":
    print("📊 Exporting processed data to CSV...")
    print("=" * 50)
    
    # Export full dataset
    success = export_to_csv()
    
    if success:
        print("\n" + "=" * 50)
        print("🎉 Export completed successfully!")
        print("\n📋 Available files:")
        print("- Full dataset: data/processed/loans_processed.csv")
        print("- Sample data: data/processed/loans_sample.csv")
        print("- Original Parquet: data/processed/loans_processed.parquet")
        
        # Also create a sample file
        print("\n🔄 Creating sample file...")
        export_sample_data()
        
    else:
        print("\n❌ Export failed. Please check the error messages above.")
