############################ ETL weekly kpis #######################
# Sales by Category
# Customer Reviews & Ratings
# Payment Method Usage
####################################################################


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("WeeklyKPIsETL").getOrCreate()


order_items = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "order_items")
    .option("user", "<RDS_USER>")
    .option("password", "<RDS_PASSWORD>")
    .load()
)
products = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "products")
    .option("user", "<RDS_USER>")
    .option("password", "<RDS_PASSWORD>")
    .load()
)
orders = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "orders")
    .option("user", "<RDS_USER>")
    .option("password", "<RDS_PASSWORD>")
    .load()
)


output_path = "s3://ecommerce-data/processed/weekly_kpis"

# Sales by Category
sales_by_category = (
    order_items.join(products, "product_id")
    .groupBy("category_id")
    .agg(sum("unit_price" * "quantity").alias("total_revenue"))
)
sales_by_category.write.mode("overwrite").parquet(f"{output_path}/sales_by_category")

# Customer Reviews & Ratings
customer_reviews = products.groupBy("product_id").agg(
    sum("rating").alias("total_ratings"),
    count("rating").alias("num_ratings"),
    (sum("rating") / count("rating")).alias("average_rating"),
)
customer_reviews.write.mode("overwrite").parquet(f"{output_path}/customer_reviews")

# Payment Method Usage
payment_method_usage = orders.groupBy("payment_method").agg(
    count("*").alias("usage_count")
)
payment_method_usage.write.mode("overwrite").parquet(
    f"{output_path}/payment_method_usage"
)
