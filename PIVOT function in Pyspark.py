# Databricks notebook source
from pyspark.sql import functions as f
mount_to_blob = "/mnt/your_mount_point"
file_name = f"{mount_to_blob}/student.csv"
file_read = spark.read.csv(file_name,header = 'True',inferSchema='True')
file_read.display()
schema_name = "id INT, name STRING, class STRING, mark STRING, gender STRING"


# COMMAND ----------

pivot_data = file_read.groupBy('class').pivot('gender').avg("mark")
pivot_data.display()

# COMMAND ----------


