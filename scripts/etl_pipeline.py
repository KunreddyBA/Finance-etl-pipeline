# scripts/etl_pipeline.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, when
from pyspark.sql.types import DoubleType, IntegerType

def run_etl():
    """
    Main ETL function to process LendingClub loan data.
    """
    spark = SparkSession.builder \
        .appName("LendingClubETL") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Define file paths
    project_root = os.path.dirname(os.path.dirname(__file__))
    input_path = os.path.join(project_root, 'data/raw/loans.csv')
    output_path = os.path.join(project_root, 'data/processed/loans_processed.parquet')

    print(f"Reading raw data from {input_path}...")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # --- Transformation ---
    print("Starting data transformation...")

    # 1. Select a subset of columns
    columns_to_keep = [
        "loan_amnt", "term", "int_rate", "grade", "emp_length", "home_ownership",
        "annual_inc", "verification_status", "issue_d", "loan_status",
        "purpose", "addr_state", "dti"
    ]
    df_selected = df.select(columns_to_keep)

    # 2. Clean and cast data types
    df_transformed = df_selected \
        .withColumn("loan_amnt", col("loan_amnt").cast(DoubleType())) \
        .withColumn("annual_inc", col("annual_inc").cast(DoubleType())) \
        .withColumn("dti", col("dti").cast(DoubleType())) \
        .withColumn("int_rate", regexp_replace(col("int_rate"), "%", "").cast(DoubleType())) \
        .withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy")) \
        .withColumn("term_months", regexp_replace(col("term"), " months", "").cast(IntegerType())) \
        .withColumn("emp_years",
            when(col("emp_length").isNull(), 0)
            .when(col("emp_length") == "< 1 year", 0)
            .when(col("emp_length") == "10+ years", 10)
            .otherwise(regexp_replace(col("emp_length"), " years?", "").cast(IntegerType()))
        )

    # 3. Drop original columns that have been cleaned
    df_final = df_transformed.drop("term", "emp_length") \
                             .filter(col("loan_status").isNotNull())

    # --- Load ---
    print(f"Writing processed data to {output_path}...")
    df_final.write.mode("overwrite").parquet(output_path)

    print("ETL process completed successfully!")
    spark.stop()

if __name__ == "__main__":
    run_etl()
