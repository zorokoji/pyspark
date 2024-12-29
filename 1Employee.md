from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

----
spark = SparkSession.builder.appName("myAPP").getOrCreate()

-----
df = spark.read.csv("emp_data.csv" , header = True)
df.show()
df.printSchema()

----
# Print column names with missing values
for column in df.columns:
  if df.filter(col(column).isNull()).count() > 0:
    print(f"{column} has missing values")

-----
# Replace missing values in the "LastName" column with "Unknown"
new_df = df.withColumn("LastName" ,when(col("LastName").isNull() , "Unknown").otherwise(col("LastName")))
new_df.show()

-----
# Drop rows with missing values in essential columns like EmpID or StartDate
df = new_df.dropna( subset = ["EmpID", "StartDate"] )

----

from pyspark.sql.functions import col, lit, greatest, least

new_df2 = df.withColumn( # cap values in range of 1 to 5
    "EmployeeRating",
    least( greatest( col("Current Employee Rating").cast("float") , lit(1)) , lit(5) )
)
df = new_df2.drop("Current Employee Rating")

-----
# Group by 'DepartmentType' and 'JobFunctionDescription' and count the number of employees
df = df.dropDuplicates()
result = df.groupby("DepartmentType" ,"JobFunctionDescription").agg(count("EmpId"))
result.show()

-----
# Group by DepartmentType and find the maximum PerformanceScore
result2 = df.groupBy("DepartmentType").agg(
    max("Performance Score").alias("MaxPerformanceScore")
)

result2.show()
