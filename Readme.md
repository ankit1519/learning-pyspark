# PySpark Full Tutorial

## Overview
This repository contains a comprehensive PySpark tutorial covering data reading, transformations, actions, SQL operations, window functions, user-defined functions (UDFs), and data writing. The tutorial is structured to provide hands-on examples using Databricks and PySpark.

## Features Covered
### 1. **Reading Data**
- Read CSV files
- Read JSON files
- Define schema using DDL and `StructType`

### 2. **Transformations**
- `SELECT` and column selection
- Aliasing (`alias`)
- Filtering (`filter` and `isin`)
- Column renaming (`withColumnRenamed`)
- Adding/modifying columns (`withColumn`)
- Regular expressions (`regexp_replace`)
- Typecasting (`cast`)
- Sorting (`orderBy`)
- Dropping columns (`drop`)
- Removing duplicates (`dropDuplicates`)

### 3. **Joins**
- Inner Join
- Left Join
- Right Join
- Anti Join

### 4. **Aggregations & Grouping**
- `groupBy` with multiple aggregations (`sum`, `avg`)
- `collect_list`
- `pivot` tables
- Conditional transformations using `when` and `otherwise`

### 5. **String Functions**
- `initcap`, `lower`, `upper`

### 6. **Date Functions**
- Current Date (`current_date`)
- Date manipulation (`date_add`, `date_sub`, `datediff`)
- Formatting (`date_format`)

### 7. **Handling Nulls**
- Dropping null values (`dropna`)
- Replacing null values (`fillna`)

### 8. **Array Operations**
- Splitting (`split`)
- Indexing
- Exploding (`explode`)
- Checking values (`array_contains`)

### 9. **Window Functions**
- Row Number (`row_number`)
- Ranking (`rank`, `dense_rank`)
- Cumulative sum (`sum` with windowing)

### 10. **User-Defined Functions (UDFs)**
- Creating and using custom functions in PySpark

### 11. **Data Writing**
- Writing to CSV
- Writing to Parquet
- Writing to Tables

### 12. **Spark SQL**
- Creating temporary views
- Running SQL queries using `spark.sql()`

## How to Use
1. Load the script into a Databricks notebook or a local PySpark environment.
2. Execute cells sequentially to understand various PySpark operations.
3. Modify file paths as per your dataset location.

## File Structure
```
|-- Spark_full_tutorial.py  # Main PySpark tutorial script
|-- README.md               # Documentation (this file)
```

## Author
Ankit Raj


