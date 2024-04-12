# Databricks notebook source
from pyspark.sql import functions as f 

data = [("John", ["apple", "banana", "orange"]),
        ("Alice", ["grapes", "peach"]),
        ("Bob", ["watermelon",None]),
        ("Joe",None)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Fruits"])
df.display()

# COMMAND ----------


explode_fun = df.select(df.Name,f.explode(df.Fruits))
explode_fun.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


explode_outer = df.select(df.Name,f.explode_outer(df.Fruits))
explode_outer.display()

# COMMAND ----------

pos_explode_outer = df.select(df.Name,f.posexplode_outer(df.Fruits))
pos_explode_outer.display()

# COMMAND ----------

pos_explode = df.select(df.Name,f.posexplode(df.Fruits))
pos_explode.display()

# COMMAND ----------


