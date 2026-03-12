# PySpark E-Commerce RDD Analysis

This project demonstrates how to analyze e-commerce transaction data using **PySpark RDD operations**.

The program performs several transformations and actions to gain insights into customer purchase behavior and product categories.

## Features

- Load transaction data into RDD
- Convert CSV records into structured tuples
- Apply RDD transformations:
  - map
  - filter
  - flatMap
- Create **Pair RDDs**
- Calculate **total spending per customer**
- Retrieve **products purchased by each customer**
- Perform **join operations** with product categories
- Save results to output files

## Technologies Used

- Python
- Apache Spark
- PySpark RDD API
- Hadoop File System (HDFS)

## Dataset Structure

Each transaction contains:
transaction_id
customer_id
product_id
product_name
category
price
quantity


Example record:


1,101,5001,Laptop,Electronics,1000.0,1


## Key PySpark Operations

- `map()` – transform records
- `filter()` – filter transactions
- `flatMap()` – extract product list
- `reduceByKey()` – calculate total spending
- `groupByKey()` – group customer purchases
- `join()` – combine product and category data

## Example Output

### High Quantity Transactions


['2', '102', '5002', 'Headphones', 'Electronics', '50.0', '2']
['3', '101', '5003', 'Book', 'Books', '20.0', '3']


### Total Spending Per Customer


('102', 250.0)
('101', 1060.0)
('103', 1000.0)


