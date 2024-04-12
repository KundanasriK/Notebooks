# Databricks notebook source
Key = dbutils.secrets.get('DatabricksScope','Blobkey')
display(Key)

# COMMAND ----------

# Replace these placeholders with your Azure Blob Storage account details
storage_account_name = "databricksdatalakeblob"
container_name = "databrickspractisedata"
storage_account_key = Key
# Mount the storage
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = "/mnt/myblobmount",
    extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": storage_account_key}
)



# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------



# COMMAND ----------

# Import the required libraries
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read DataFrame from Blob CSV") \
    .getOrCreate()

# Define the mount point
mount_point = "/mnt/blobmount"

# Define the path to the CSV file within the mount point
csv_path = f"{mount_point}/EmployeeData1.csv"
# Replace "/path/to/your/file.csv" with the actual path to your CSV file within the mount point

# Read the CSV file into a DataFrame
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Show the DataFrame
df.show()


# COMMAND ----------

#Filter Null value from above file
from pyspark.sql.functions import col
filter_is_Null = df.filter(col("MANAGER_ID").isNull())
filter_is_Null.show()

# COMMAND ----------

#Filters all null values from all columns
df_filtered = df.dropna()
df_filtered.show()

# COMMAND ----------

df_is_not_null = df.filter(col("MANAGER_ID").isNotNull())
df_is_not_null.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

total_salary_by_dept = df.groupBy("DEPARTMENT_ID").agg(F.sum("SALARY").alias("TotalSalary")) 
total_salary_by_dept.show()

# COMMAND ----------

from pyspark.sql import functions as F

total_salary_by_manager = df_is_not_null.groupBy('MANAGER_ID').agg(F.sum('SALARY').alias('TOTAL SALARY'))
total_salary_by_manager.show()

# COMMAND ----------

from pyspark.sql import functions as F
collect_list_from_col = df_is_not_null.groupBy('MANAGER_ID').agg(F.collect_list('EMPLOYEE_ID'))
collect_JobId_from_Man = df_is_not_null.groupBy('MANAGER_ID').agg(F.collect_list('JOB_ID'))
collect_JobId_from_Man.show()

# COMMAND ----------

collectset_JobId_from_Man = df_is_not_null.groupBy('MANAGER_ID').agg(F.collect_set('JOB_ID'))
collectset_JobId_from_Man.show()

# COMMAND ----------

from pyspark.sql import functions as F
approx_distinct = df_is_not_null.select(F.approx_count_distinct('MANAGER_ID').alias('count of distinct Manager'))
approx_distinct.show()

# COMMAND ----------

from pyspark.sql import functions as F
max_salary = df_is_not_null.select(F.max("SALARY"))
min_salary = df_is_not_null.select(F.min("SALARY"))
max_salary.show()
min_salary.show()

# COMMAND ----------

    from pyspark.sql import functions as F
    sum_fun = df_is_not_null.groupby('MANAGER_ID').agg(F.sum('SALARY'))
    sum_fun.show()

# COMMAND ----------

from pyspark.sql import functions as F
avg_fun = df_is_not_null.groupby('MANAGER_ID').agg(F.avg('SALARY'))
avg_fun.show()

# COMMAND ----------

from pyspark.sql import functions as F
count_fun = df_is_not_null.groupby('MANAGER_ID').count()
count_fun.show()

# COMMAND ----------

from pyspark.sql import functions as F
like_oper = df_is_not_null.filter((df_is_not_null['MANAGER_ID']).like('108'))
not_like_oper = df_is_not_null.filter(~(df_is_not_null['MANAGER_ID']).like('108'))
not_like_oper.show()

# COMMAND ----------

from pyspark.sql import functions as F
between_data = df_is_not_null.filter(df_is_not_null['MANAGER_ID'].between('100','110'))
between_data.show()

# COMMAND ----------


