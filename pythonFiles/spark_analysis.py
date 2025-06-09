from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, regexp_extract, sum as _sum, count
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

# Initialize Spark session with Hive support
spark = SparkSession.builder.appName("eCommerce Analysis").enableHiveSupport().getOrCreate()

# Load tables
spark.sql("use sixfive_db")
customers_df = spark.table("customers")
products_df = spark.table("products")
orders_df = spark.table("orders")

# Cleaning: Replace nulls (assuming generated data is clean, but as an example)
customers_df = customers_df.na.fill({"name": "Unknown", "phone": "N/A", "address": "[]"})
products_df = products_df.na.fill({"product_name": "Unknown", "product_category": "Other", "product_price": 0.0})
orders_df = orders_df.na.fill({"products": "[]", "total_order_amount": 0.0, "returned": "False"})

# Define schema for products JSON
products_schema = ArrayType(StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True)
]))

# Parse products JSON and explode
orders_expanded_df = orders_df.withColumn("products_array", from_json(col("products"), products_schema)) \
                             .withColumn("product", explode(col("products_array"))) \
                             .select(
                                 col("order_id"), col("customer_id"), col("total_order_amount"),
                                 col("order_date"), col("returned"),
                                 col("product.product_id"), col("product.quantity")
                             )

# Transformations
# 1. Customers with high purchases
top_customers = orders_df.groupBy("customer_id") \
                         .agg(_sum("total_order_amount").alias("total_spent")) \
                         .join(customers_df, "customer_id") \
                         .select("customer_id", "name", "total_spent") \
                         .orderBy(col("total_spent").desc()) \
                         .limit(5)
top_customers.show()

# 2. Top selling products (by quantity)
top_products = orders_expanded_df.groupBy("product_id") \
                                 .agg(_sum("quantity").alias("total_quantity")) \
                                 .join(products_df, "product_id") \
                                 .select("product_id", "product_name", "total_quantity") \
                                 .orderBy(col("total_quantity").desc()) \
                                 .limit(5)
top_products.show()

# 3. City with most customers
customers_df_with_city = customers_df.withColumn("city", regexp_extract(col("address"), r'\["[^"]+", "[^"]+", "([^"]+)", "[^"]+"\]', 1))
top_cities = customers_df_with_city.groupBy("city") \
                                   .agg(count("*").alias("customer_count")) \
                                   .orderBy(col("customer_count").desc()) \
                                   .limit(5)
top_cities.show()

# 4. Highest order value
highest_order = orders_df.orderBy(col("total_order_amount").desc()).limit(1)
highest_order.show()

# 5. Customer who purchased the most (same as 1, assuming by amount)
top_customer = top_customers.limit(1)
top_customer.show()

# 6. Most popular product category
top_categories = orders_expanded_df.join(products_df, "product_id") \
                                   .groupBy("product_category") \
                                   .agg(_sum("quantity").alias("total_quantity")) \
                                   .orderBy(col("total_quantity").desc()) \
                                   .limit(5)
top_categories.show()

# 7. Products with most returns
returned_products = orders_expanded_df.filter(col("returned") == "True") \
                                      .groupBy("product_id") \
                                      .agg(count("*").alias("return_count")) \
                                      .join(products_df, "product_id") \
                                      .select("product_id", "product_name", "return_count") \
                                      .orderBy(col("return_count").desc()) \
                                      .limit(5)
returned_products.show()

# Stop Spark session
spark.stop()