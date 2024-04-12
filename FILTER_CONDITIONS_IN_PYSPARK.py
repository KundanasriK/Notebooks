# Databricks notebook source
from pyspark.sql import functions as f

mount = "/mnt/your_mount_point"
blob_file = f"{mount}/EmployeeData2.csv"
df = spark.read.csv(blob_file,header = "True")
df.display()


# COMMAND ----------

# FILTER DATA BASED ON JOB and MANAGER ID

clerk_list = df.filter(f.column('JOB_ID').contains('ST_CLERK'))
manager_id = df.groupby('MANAGER_ID').agg(f.col('MANAGER_ID').like('100'))
manager_id.show()

# COMMAND ----------

dep_high_sal = df.filter((f.col('SALARY') > '2000')& (f.col('DEPARTMENT_ID') =='30'))
dep_high_sal.display()

# COMMAND ----------

data_check = df.filter(f.col('PHONE_NUMBER').contains('650'))
data_check.display()

# COMMAND ----------

data_Starts_with = df.filter(f.col('FIRST_NAME').startswith('J'))
data_Starts_with.display()

# COMMAND ----------


