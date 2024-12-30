from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("Question3").getOrCreate()
df3 = spark.read.csv("Cleaned_DS_Jobs.csv", header=True)

----

df3 = df3.withColumn("Min_salaray" , split(col("Salary Estimate") , "-").getItem(0).cast("Int"))
df3 = df3.withColumn("Max_salaray" , split(col("Salary Estimate") , "-").getItem(1).cast("Int"))
df.show()

---
df3 = df3.withColumn("Average_salary" , (col("Min_salaray") + col("Max_salaray")) / 2)

--
df3 = df3.withColumn("Rating" , when( (col("Rating")== -1) | (col("Rating")== 0)  , 1).otherwise(col("Rating")))

--
df3 = df3.fillna(-1)

----
df3_group = df3.groupby("Job Title").agg(avg("Average_salary").alias("Average_salary"))

----
result = df3.groupby("Size").agg(avg("Average_salary").alias("Average_salary"))
result.show()

----
result.write.csv("path/to/output_directory", header=True)
