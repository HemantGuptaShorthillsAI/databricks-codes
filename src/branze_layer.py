# Databricks notebook source
# COMMAND ----------

# Define DBFS paths for raw CSV files
# Replace these with the actual paths to your CSV files
csv_path_1 = "/mnt/path/to/your/csv_file_1.csv"
csv_path_2 = "/mnt/path/to/your/csv_file_2.csv"
csv_path_3 = "/mnt/path/to/your/csv_file_3.csv"

# Define the Delta table output path
delta_output_path = "/FileStore/tables/paramdeep/task_tables4"

# COMMAND ----------

# Read the first CSV file into a DataFrame with inferred schema
df_1 = spark.read.csv(csv_path_1, header=True, inferSchema=True)

# Write the DataFrame as a Delta table
df_1.write.format("delta").mode("overwrite").save(delta_output_path + "/table_1")

# COMMAND ----------

# Read the second CSV file into a DataFrame with inferred schema
df_2 = spark.read.csv(csv_path_2, header=True, inferSchema=True)

# Write the DataFrame as a Delta table
df_2.write.format("delta").mode("overwrite").save(delta_output_path + "/table_2")

# COMMAND ----------

# Read the third CSV file into a DataFrame with inferred schema
df_3 = spark.read.csv(csv_path_3, header=True, inferSchema=True)

# Write the DataFrame as a Delta table
df_3.write.format("delta").mode("overwrite").save(delta_output_path + "/table_3")

# COMMAND ----------

print("Bronze layer ETL script completed successfully!")