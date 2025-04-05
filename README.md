# Retail-Sales-Data-Analysis-with-PySpark


# Table of Contents:
-[Overview](#Overview)

-[Dataset](#Dataset)

-[Techniques Used](#Techniiques-Used)

-[Implementation](#Implementation)

-[About the Project](#AbouttheProject)

* * *

# Overview:
This project analyzes large-scale retail sales data using PySpark, focusing on deriving key business insights such as top-selling products, monthly revenue trends, and customer spending behavior. It demonstrates hands-on experience with Big Data tools and aligns with real-world data engineering roles.

# Dataset:
You can download the dataset [here] (https://www.kaggle.com/datasets/vijayuv/onlineretail)

* * *

# Tecniques Used

1. Data Ingestion
Tool: PySpark (spark.read)

Purpose: To load large-scale data efficiently from a CSV file into a Spark DataFrame.

Techniques Used:

Schema inference with inferSchema=True

Header detection using header=True


 2. Data Cleaning
Tool: PySpark DataFrame transformations

Purpose: To ensure data quality before analysis.

Techniques Used:

dropna() – Removes rows with null values

dropDuplicates() – Eliminates duplicate records

to_date() – Converts InvoiceDate from string to date format

Type conversion – Ensures numeric columns (Quantity, UnitPrice) are properly typed.


3. Feature Engineering
Tool: Spark SQL functions

Purpose: To create additional variables that are useful for analysis.

Techniques Used:

withColumn() – Adds new columns:

TotalAmount = Quantity * UnitPrice

Month and Year from InvoiceDate


4. Data Aggregation & Business Logic
Tool: SparkSQL / DataFrame operations

Purpose: To derive business insights from large datasets.

Techniques Used:

groupBy() – Grouping data for aggregation

_sum(), count() – Aggregating sales, quantities

orderBy(desc()) – Sorting to find top-performing products/customers

limit() – Selecting top N records

year(), month() – Extract temporal features


* * *

# Implementation:

Top 10 selling products by quantity

Monthly revenue trends by Year and Month

Customer segmentation based on total spending

* * *

# About the Project:
Retail Sales Data Analysis Using PySpark is a Big Data project that processes large-scale retail transaction data to uncover key business insights. Using PySpark, it performs data cleaning, feature engineering, and analysis to identify top-selling products, monthly revenue trends, and customer spending patterns. The project demonstrates efficient use of Spark SQL and DataFrame operations for scalable data processing and insight generation.



