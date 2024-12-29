from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Question3").getOrCreate()

df = spark.read.csv("Cleaned_DS_Jobs.csv", header=True)

df.show()
----
df = df.withColumn( "min_salary", split(col("Salary Estimate"), "-").getItem(0).cast("int") # Extract min
).withColumn(
"max_salary", split(col("Salary Estimate"), "-").getItem(1).cast("int") # Extract max
)
df.show()
---
# Assuming your DataFrame is named 'df_split_salary'
df = df.withColumn(
"average_salary", (col("min_salary") + col("max_salary")) / 2
)
df.show()

--
# Replace -1 and 0 in the Rating column with 1
df = df.withColumn(
"Rating",
when((col("Rating") == -1) | (col("Rating") == 0), 1).otherwise(col("Rating"))
)
--
df_transformed = df.fillna(-1)
----
df_grouped = df.groupBy("Job Title").agg(
avg("average_salary")
)
# Show the result
df_grouped.show()
----
df_avg_salary_by_size = df_transformed.groupBy("Size").agg(
avg("average_salary")
)
# Show the result
df_avg_salary_by_size.show()
----
