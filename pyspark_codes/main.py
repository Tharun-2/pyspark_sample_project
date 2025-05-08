# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# import shutil

# # Define directories
# SOURCE_DIR = "/home/mt24052/pyspark_file_watcher/input"
# DEST_DIR = "/home/mt24052/pyspark_file_watcher/output"
# ARCHIVE_DIR = "/home/mt24052/pyspark_file_watcher/archive"

# # Initialize Spark
# spark = SparkSession.builder.appName("CSVtoParquetPipeline").getOrCreate()

# # List CSV files
# csv_files = [f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")]

# for file in csv_files:
#     file_path = os.path.join(SOURCE_DIR, file)
    
#     # Read CSV
#     df = spark.read.option("header", True).csv(file_path)

#     # Example transformation: rename and filter
#     df_transformed = df.withColumnRenamed("old_column", "new_column").filter(col("some_column") != "unwanted_value")

#     # Save as Parquet
#     output_path = os.path.join(DEST_DIR, file.replace(".csv", ".parquet"))
#     df_transformed.write.mode("overwrite").parquet(output_path)

#     # Move original to archive
#     shutil.move(file_path, os.path.join(ARCHIVE_DIR, file))

# spark.stop()


import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define directories
SOURCE_DIR = "/home/mt24052/pyspark_file_watcher/input"
DEST_DIR = "/home/mt24052/pyspark_file_watcher/output"
ARCHIVE_DIR = "/home/mt24052/pyspark_file_watcher/archive"

# Transformation rules
RENAME_MAP = {"plant_name": "plant"}
FILTER_CONDITION = ("UOM", "==", "I")  # (column, operator, value)

# Start Spark session
spark = SparkSession.builder.appName("SafeCSVtoParquetPipeline").getOrCreate()

# Process CSV files
csv_files = [f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")]

for file in csv_files:
    file_path = os.path.join(SOURCE_DIR, file)
    print(f"\nProcessing: {file_path}")

    # Read CSV
    df = spark.read.option("header", True).csv(file_path)
    print("Initial columns:", df.columns)

    # Rename columns if they exist
    for old_col, new_col in RENAME_MAP.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)
            print(f"Renamed column: {old_col} → {new_col}")
        else:
            print(f"[WARNING] Column '{old_col}' not found — skipping rename.")

    # Apply filter only if column exists
    filter_col, op, val = FILTER_CONDITION
    if filter_col in df.columns:
        if op == "!=":
            df = df.filter(col(filter_col) != val)
        elif op == "==":
            df = df.filter(col(filter_col) == val)
        print(f"Applied filter: {filter_col} {op} {val}")
        print(df.show())
    else:
        print(f"[WARNING] Column '{filter_col}' not found — skipping filter.")

    # Save to Parquet
    output_path = os.path.join(DEST_DIR, file.replace(".csv", ".parquet"))
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved to: {output_path}")

    # Archive original
    shutil.move(file_path, os.path.join(ARCHIVE_DIR, file))

spark.stop()
