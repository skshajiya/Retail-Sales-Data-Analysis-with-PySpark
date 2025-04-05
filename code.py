# Retail Sales Data Analysis Using PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, desc, to_date, month, year

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Retail Sales Analysis") \
    .getOrCreate()

# Step 2: Load Dataset
# Replace 'retail_sales.csv' with your file path
df = spark.read.option("header", True).option("inferSchema", True).csv("retail_sales.csv")

# Step 3: Data Cleaning
df_cleaned = df.dropna() \
    .dropDuplicates() \
    .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "yyyy-MM-dd")) \
    .withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))

# Step 4: Feature Engineering
df_features = df_cleaned.withColumn("Year", year("InvoiceDate")) \
    .withColumn("Month", month("InvoiceDate"))

# Step 5: Business Insights

# 5.1 Top 10 Selling Products
top_products = df_features.groupBy("Description") \
    .agg(_sum("Quantity").alias("TotalQuantity")) \
    .orderBy(desc("TotalQuantity")) \
    .limit(10)

# 5.2 Monthly Revenue
monthly_revenue = df_features.groupBy("Year", "Month") \
    .agg(_sum("TotalAmount").alias("MonthlyRevenue")) \
    .orderBy("Year", "Month")

# 5.3 Customer Segmentation by Spending
customer_spending = df_features.groupBy("CustomerID") \
    .agg(_sum("TotalAmount").alias("TotalSpent")) \
    .orderBy(desc("TotalSpent"))

# Step 6: Save Output to CSV
top_products.write.mode("overwrite").csv("output/top_products", header=True)
monthly_revenue.write.mode("overwrite").csv("output/monthly_revenue", header=True)
customer_spending.write.mode("overwrite").csv("output/customer_spending", header=True)

# Optional: Show sample outputs
top_products.show()
monthly_revenue.show()
customer_spending.show()

# Stop Spark Session
spark.stop()
