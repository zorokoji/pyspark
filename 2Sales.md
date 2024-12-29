from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Question2").getOrCreate()
df = spark.read.csv('Sales Data.csv', header=True)

--

for column in df.columns:
  if df.filter(col(column).isNull()).count() > 0:
    print(column)
    
---

# Fill missing values in the specific column with the calculated mean
colName = "Sales"
mean_val = df2.select(mean(colName)).collect()[0][0] 
df2 = df2.fillna({colName: mean_val})

--
df=df.dropDuplicates()

---
df = df.withColumn("Quantity Ordered", col("Quantity Ordered").cast("Integer"))
df = df.withColumn("Price Each", col("Price Each").cast("Float"))
df = df.withColumn("Sales", col("Sales").cast("Float"))

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
df_total_sales.show()
