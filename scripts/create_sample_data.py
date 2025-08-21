# scripts/create_sample_data.py

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def create_sample_data():
    """
    Create sample LendingClub data for testing the ETL pipeline.
    """
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Number of sample records
    n_records = 1000
    
    # Generate sample data
    data = {
        'loan_amnt': np.random.randint(5000, 35000, n_records),
        'term': np.random.choice([' 36 months', ' 60 months'], n_records),
        'int_rate': [f"{np.random.uniform(5.0, 25.0):.1f}%" for _ in range(n_records)],
        'grade': np.random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G'], n_records, p=[0.2, 0.25, 0.2, 0.15, 0.1, 0.07, 0.03]),
        'emp_length': np.random.choice(['< 1 year', '1 year', '2 years', '3 years', '4 years', '5 years', '6 years', '7 years', '8 years', '9 years', '10+ years'], n_records),
        'home_ownership': np.random.choice(['RENT', 'OWN', 'MORTGAGE'], n_records, p=[0.4, 0.2, 0.4]),
        'annual_inc': np.random.randint(30000, 150000, n_records),
        'verification_status': np.random.choice(['Verified', 'Source Verified', 'Not Verified'], n_records),
        'issue_d': [f"{np.random.choice(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])}-{np.random.randint(2015, 2019)}" for _ in range(n_records)],
        'loan_status': np.random.choice(['Fully Paid', 'Charged Off', 'Current', 'Late (31-120 days)', 'Late (16-30 days)'], n_records, p=[0.6, 0.1, 0.2, 0.05, 0.05]),
        'purpose': np.random.choice(['debt_consolidation', 'credit_card', 'home_improvement', 'major_purchase', 'small_business', 'car', 'medical', 'vacation'], n_records),
        'addr_state': np.random.choice(['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI'], n_records),
        'dti': np.random.uniform(5.0, 35.0, n_records)
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Define file path
    project_root = os.path.dirname(os.path.dirname(__file__))
    output_path = os.path.join(project_root, 'data/raw/loans.csv')
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"Sample data created with {n_records} records")
    print(f"Saved to: {output_path}")
    print("\nSample data preview:")
    print(df.head())
    
    return output_path

if __name__ == "__main__":
    create_sample_data()
