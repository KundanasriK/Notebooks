# Databricks notebook source
from pyspark.sql import functions as f
mount_to_blob = "/mnt/your_mount_point"
file_name = f"{mount_to_blob}/student.csv"
file_read = spark.read.csv(file_name,header = 'True')
file_read.display()

# COMMAND ----------

#Filter the Merit candidates based on marks
from pyspark.sql.functions import when
merit_A = file_read.withColumn('RANK',when(file_read.mark > 90,'A')
                     .when(file_read.mark > 70,'B')
                     .when(file_read.mark > 50,'C')
                     .otherwise('Fail'))
merit_A.display()


# COMMAND ----------

from pyspark.sql import functions as f 
girls_list = file_read.groupBy('gender').agg((f.sum(file_read.mark)*f.count(file_read.gender))/100).withColumnRenamed('((sum(mark) * count(gender)) / 100)','Average')
girls_list.display()

# COMMAND ----------


