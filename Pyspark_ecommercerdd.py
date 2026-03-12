from pyspark.sql import SparkSession


def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("ECommerceTransactionsRDDAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext

    # Sample transaction data
    data = [
        "1,101,5001,Laptop,Electronics,1000.0,1",
        "2,102,5002,Headphones,Electronics,50.0,2",
        "3,101,5003,Book,Books,20.0,3",
        "4,103,5004,Laptop,Electronics,1000.0,1",
        "5,102,5005,Chair,Furniture,150.0,1"
    ]

    # Load data into RDD
    transactions_rdd = sc.parallelize(data)

    # Convert CSV string into list/tuple-like structure
    transactions_tuple_rdd = transactions_rdd.map(lambda line: line.split(","))

    # Filter transactions where quantity > 1
    high_quantity_rdd = transactions_tuple_rdd.filter(lambda x: int(x[6]) > 1)

    # Extract all product names
    products_flat_rdd = transactions_tuple_rdd.flatMap(lambda x: [x[3]])

    # Create Pair RDD: (customer_id, (product_name, total_price))
    pair_rdd = transactions_tuple_rdd.map(
        lambda x: (x[1], (x[3], float(x[5]) * int(x[6])))
    )

    # Total spending by each customer
    customer_spending_rdd = pair_rdd \
        .map(lambda x: (x[0], x[1][1])) \
        .reduceByKey(lambda a, b: a + b)

    # Products purchased by each customer
    customer_products_rdd = pair_rdd \
        .map(lambda x: (x[0], x[1][0])) \
        .groupByKey() \
        .mapValues(list)

    # Product-category mapping RDD
    product_category_data = [
        ("Laptop", "Electronics"),
        ("Headphones", "Electronics"),
        ("Book", "Books"),
        ("Chair", "Furniture")
    ]
    product_category_rdd = sc.parallelize(product_category_data)

    # Join transaction info with category info
    customer_product_category_rdd = pair_rdd \
        .map(lambda x: (x[1][0], (x[0], x[1][1]))) \
        .join(product_category_rdd)

    # Collect results
    total_spending = customer_spending_rdd.collect()
    products_per_customer = customer_products_rdd.collect()
    product_category_info = customer_product_category_rdd.collect()
    high_quantity_transactions = high_quantity_rdd.collect()
    all_products = products_flat_rdd.collect()

    # Print results
    print("=== High Quantity Transactions (quantity > 1) ===")
    for row in high_quantity_transactions:
        print(row)

    print("\n=== All Products Purchased ===")
    for product in all_products:
        print(product)

    print("\n=== Total Spending Per Customer ===")
    for row in total_spending:
        print(row)

    print("\n=== Products Purchased Per Customer ===")
    for row in products_per_customer:
        print(row)

    print("\n=== Product Category Join Results ===")
    for row in product_category_info:
        print(row)

    # Save results
    customer_spending_rdd.saveAsTextFile("output/customer_spending")
    customer_products_rdd.saveAsTextFile("output/customer_products")
    customer_product_category_rdd.saveAsTextFile("output/customer_product_category")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
