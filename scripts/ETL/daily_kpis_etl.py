############################ ETL daily kpis #######################
# Total Revenue
# Average Order Value (AOV)
# Revenue Growth Rate
# Refund Rate
# Abandoned Cart Rate
# Top-Selling Products
# Low Stock Products
# Product Conversion Rate
###################################################################


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("DailyKPIsETL").getOrCreate()

# Load Data from RDS
orders = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "orders")
    .option("user", "<RDS_USER>")
    .option("password", "<RDS_PASSWORD>")
    .load()
)
order_items = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "order_items")
    .option("user", "<RDS_USER>")
    .option("password", "<RDS_PASSWORD>")
    .load()
)
cart_items = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "cart_items")
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
product_views = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "product_views")
    .option("user", "<RDS_USER>")
    .option("password", "<RDS_PASSWORD>")
    .load()
)

output_path = "s3://ecommerce-data/processed/daily_kpis"

# Total Revenue
total_revenue = order_items.withColumn(
    "revenue", col("unit_price") * col("quantity")
).agg(sum("revenue").alias("total_revenue"))
total_revenue.write.mode("overwrite").parquet(f"{output_path}/total_revenue")

# Average Order Value (AOV)
aov = (
    orders.join(order_items, "order_id")
    .withColumn("revenue", col("unit_price") * col("quantity"))
    .groupBy("order_id")
    .agg(sum("revenue").alias("order_value"))
    .agg(sum("order_value") / count("order_id").alias("average_order_value"))
)
aov.write.mode("overwrite").parquet(f"{output_path}/aov")

# Revenue Growth Rate (Current vs Previous Period)
current_revenue = (
    order_items.filter(col("order_date") >= lit("2023-12-01"))
    .withColumn("revenue", col("unit_price") * col("quantity"))
    .agg(sum("revenue").alias("current_revenue"))
)
previous_revenue = (
    order_items.filter(col("order_date") < lit("2023-12-01"))
    .withColumn("revenue", col("unit_price") * col("quantity"))
    .agg(sum("revenue").alias("previous_revenue"))
)
revenue_growth = current_revenue.crossJoin(previous_revenue).withColumn(
    "revenue_growth_rate",
    (col("current_revenue") - col("previous_revenue")) / col("previous_revenue"),
)
revenue_growth.write.mode("overwrite").parquet(f"{output_path}/revenue_growth")

# Refund Rate
refund_rate = orders.filter(col("status") == "Refunded").agg(
    (count("*") / orders.count() * 100).alias("refund_rate")
)
refund_rate.write.mode("overwrite").parquet(f"{output_path}/refund_rate")

# Abandoned Cart Rate
abandoned_cart_rate = cart_items.filter(col("status") == "Abandoned").agg(
    (count("*") / cart_items.count() * 100).alias("abandoned_cart_rate")
)
abandoned_cart_rate.write.mode("overwrite").parquet(
    f"{output_path}/abandoned_cart_rate"
)

# Top-Selling Products
top_selling_products = (
    order_items.groupBy("product_id")
    .agg(sum("quantity").alias("total_sales"))
    .join(products, "product_id")
    .orderBy(col("total_sales").desc())
)
top_selling_products.write.mode("overwrite").parquet(
    f"{output_path}/top_selling_products"
)

# Low Stock Products
low_stock_products = products.filter(col("stock_quantity") < 10)
low_stock_products.write.mode("overwrite").parquet(f"{output_path}/low_stock_products")

# Product Conversion Rate
product_conversion_rate = (
    product_views.groupBy("product_id")
    .agg(count("*").alias("views"))
    .join(
        order_items.groupBy("product_id").agg(count("*").alias("orders")), "product_id"
    )
    .withColumn("conversion_rate", (col("orders") / col("views")) * 100)
)
product_conversion_rate.write.mode("overwrite").parquet(
    f"{output_path}/product_conversion_rate"
)
