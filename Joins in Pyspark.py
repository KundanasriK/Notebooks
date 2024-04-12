# Databricks notebook source
from pyspark.sql import functions as f
mount_point = '/mnt/your_mount_point'
file_name = f"{mount_point}/EmployeeData1.csv"
df = spark.read.csv(file_name,header = 'TRUE')
df_manager = df.filter(f.col('MANAGER_ID').like('100')).drop('JOB_ID')
df_dept = df.filter(f.col('DEPARTMENT_ID').isin('20','30')).drop('EMPLOYEE_ID','FIRST_NAME','LAST_NAME','EMAIL','PHONE_NUMBER','HIRE_DATE','SALARY','COMMISSION_PCT','MANAGER_ID')
df_manager.display()
df_dept.display()

# COMMAND ----------

#appplying joins for dataframes

inner_join = df_manager.join(df_dept, df_manager.DEPARTMENT_ID == df_dept.DEPARTMENT_ID,"inner").distinct()
inner_join.display()

# COMMAND ----------

# applying full outer join

outer_join = df_manager.join(df_dept, df_manager.DEPARTMENT_ID == df_dept.DEPARTMENT_ID,"outer").distinct()
outer_join.display()


# COMMAND ----------

# applying left outer join

leftouter_join = df_manager.join(df_dept, df_manager.DEPARTMENT_ID == df_dept.DEPARTMENT_ID,"leftouter").distinct()
leftouter_join.display()

# COMMAND ----------

# applying right outer join

rightouter_join = df_manager.join(df_dept, df_manager.DEPARTMENT_ID == df_dept.DEPARTMENT_ID,"rightouter").distinct()
rightouter_join.display()

# COMMAND ----------


