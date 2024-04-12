# Databricks notebook source
File_path = "dbfs:/FileStore/tables/Sampledata/Credit_Card_Trans.csv"
Load_Data = spark.read.format("csv").options(header = 'true').load(File_path)
display(Load_Data)

# COMMAND ----------

Load_Data.printSchema()

# COMMAND ----------

Schema = 'CardTypeCode String,IssuingBank String,CardNumber String,HolderName String,CreditLimit Integer'

# COMMAND ----------

File_path = "dbfs:/FileStore/tables/Sampledata/Credit_Card_Trans.csv"
Load_Data = spark.read.format("csv").schema(Schema).options(header = 'true').load(File_path)
display(Load_Data)

# COMMAND ----------

Load_Data.printSchema()

# COMMAND ----------


