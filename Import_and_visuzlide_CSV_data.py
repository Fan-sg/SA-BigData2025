# Databricks notebook source
import pandas as pd

# Path to the uploaded file in your workspace
file_path = "/Workspace/Users/liufan.iss.demo@gmail.com/MyProject/Baby_Names__Beginning_2007.csv"

# Read the CSV into a pandas DataFrame
df = pd.read_csv(file_path)

# Show first 5 rows
df.head()





# COMMAND ----------

# Convert pandas DataFrame to Spark DataFrame
df_spark = spark.createDataFrame(df)

# Show first 5 rows
df_spark.show(5)


# COMMAND ----------

# Create a temporary view for SQL queries
df_spark.createOrReplaceTempView("baby_names")

# Example SQL query
spark.sql("SELECT * FROM baby_names LIMIT 10").show()


# COMMAND ----------

display(df_spark)

# COMMAND ----------

