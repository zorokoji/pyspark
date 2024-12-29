from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
----
spark=SparkSession.builder.appName("Question1").getOrCreate()
----
df = spark.read.csv("emp_data.csv", header=True)
df.show()
----
print("Columns with missing values:")
# Print column names with missing values
for column in df.columns:
  if df.filter(col(column).isNull()).count() > 0:
    print(f"{column} has missing values")
-----
# Replace missing values in the "LastName" column with "Unknown"
df_replaced = df.withColumn("LastName", when(col("LastName").isNull(), "Unknown").otherwise(col("LastName")))
print("DataFrame after replacing missing values in LastName:")
df_replaced.show()
-----
# Drop rows with missing values in essential columns like EmpID or StartDate
df = df_replaced.dropna(subset=["EmpID", "StartDate"])
print("DataFrame after dropping rows with missing values in EmpID or StartDate:")
df.show()
----

from pyspark.sql.functions import col, lit, greatest, least
# Cap values between 1 and 5 for the 'CurrentEmployeeRating' column
df_outliers_handled = df.withColumn(
    "CurrentEmployeeRating",
    least(greatest(col("Current Employee Rating").cast("float"), lit(1)), lit(5))
)
# Drop the original 'Current Employee Rating' column and show the result
df = df_outliers_handled.drop("Current Employee Rating")
df.show()

-----

from pyspark.sql.functions import count
# Remove duplicate records
df = df.dropDuplicates()
# Group by 'DepartmentType' and 'JobFunctionDescription' and count the number of employees
df_employee_count = df.groupBy("DepartmentType", "JobFunctionDescription").agg(count("EmpID"))
# Show the result
df_employee_count.show()

df.show()
-----

from pyspark.sql.functions import max
# Group by DepartmentType and find the maximum PerformanceScore
result = df.groupBy("DepartmentType").agg(
    max("Performance Score").alias("MaxPerformanceScore")
)
# Show the result
result.show()
