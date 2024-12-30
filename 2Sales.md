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
df_total_sales = df.groupBy("Product").agg( sum("Sales").alias("TotalSales"))
df_total_sales.show()


---
#Tableau
Product vs dt
Order date in Month vs dt
City vs dt -> in filter add city and top 10 , sales and choose count
Product vs dt …. City for colour
Product colour , city to  detail , dt to  size  
Product to filter , top 5 , sales and sum | order date , mont vs dt | product to color

