from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("myAPP").getOrCreate()
df = spark.read.csv("emp_data.csv" , header = True)

----
for column in df.columns:
  if df.filter(col(column).isNull()).count() > 0:
    print(f"{column} has missing values")

-----
new_df = df.withColumn("LastName" ,when(col("LastName").isNull() , "Unknown").otherwise(col("LastName")))
new_df.show()

-----
df = new_df.dropna( subset = ["EmpID", "StartDate"] )

----

from pyspark.sql.functions import col, lit, greatest, least
new_df2 = df.withColumn( # cap values in range of 1 to 5
    "EmployeeRating",
    least( greatest( col("Current Employee Rating").cast("float") , lit(1)) , lit(5) )
)
df = new_df2.drop("Current Employee Rating")

-----
df = df.dropDuplicates()
result = df.groupby("DepartmentType" ,"JobFunctionDescription").agg(count("EmpId"))
result.show()

-----
result2 = df.groupBy("DepartmentType").agg(
    max("Performance Score").alias("MaxPerformanceScore")
)

result2.show()



-----
#Tableau
Gender code vs csv ..gender to color
Race desc to color and css to size ..then pi chart ..choose entire view
Title vs csv
Division to color , csv to size … treemap
Analysis , calculation , YEAR … DATETIME([Start Time]) vs csv
