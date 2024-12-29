from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Question2").getOrCreate()

df = spark.read.csv('Sales Data.csv', header=True)

df.show()
--
for column in df.columns:
  if df.filter(col(column).isNull()).count() > 0:
    print(column)
---
column_name = 'Sales' # Replace with your column name
# Calculate the mean for the column
mean_value = df.select(mean(column_name)).collect()[0][0]
# Fill missing values in the specific column with the calculated mean
df = df.fillna({column_name: mean_value})
---
df=df.dropDuplicates()
---
df = df.withColumn("Quantity Ordered", col("Quantity Ordered").cast("Integer"))
df = df.withColumn("Price Each", col("Price Each").cast("Float"))
df = df.withColumn("Sales", col("Sales").cast("Float"))

---
df.printSchema()
---
df = df.filter(
(col("Sales") >= 0) &
(col("Price Each") >= 0) &
(col("Quantity Ordered") >= 0)
)
---
df_total_sales = df.groupBy("Product").agg(
sum("Sales").alias("TotalSales")
)
# Show the result
df_total_sales.show()
