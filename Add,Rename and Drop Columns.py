# Databricks notebook source
from pyspark.sql import functions as f
mount = "/mnt/your_mount_point"
file_path = f"{mount}/EmployeeData1.csv"
file_read = spark.read.csv(file_path,header = 'True')
file_read.display()

# COMMAND ----------

add_column = file_read.withColumn('Organisation',f.lit('Spark'))
add_column.display()

# COMMAND ----------

add_bonus = add_column.withColumn('Bonus',add_column.SALARY*0.1).withColumn('Name',f.concat(add_column.FIRST_NAME, f.lit(' ') ,add_column.LAST_NAME)).drop(file_read.COMMISSION_PCT)
add_bonus.display()

# COMMAND ----------


