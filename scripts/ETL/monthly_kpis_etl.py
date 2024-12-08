############################ ETL Monthly kpis #######################
# Customer Churn Rate
# Conversion Funnel Analysis
#####################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark Session
spark = SparkSession.builder.appName("MonthlyKPIsETL").getOrCreate()

output_path = "s3://ecommerce-data/processed/monthly_kpis"

orders = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "orders")
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
cart_items = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://<RDS_HOST>:5432/ecommerce")
    .option("dbtable", "cart_items")
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

# Customer Churn Rate
churn_rate = (
    orders.groupBy("user_id")
    .agg(count("*").alias("total_orders"))
    .filter(col("total_orders") == 1)
    .agg((count("*") / orders.count() * 100).alias("churn_rate"))
)
churn_rate.write.mode("overwrite").parquet(f"{output_path}/churn_rate")

# Conversion Funnel Analysis
conversion_funnel = (
    product_views.groupBy("product_id")
    .agg(count("*").alias("views"))
    .join(
        cart_items.groupBy("product_id").agg(count("*").alias("cart_adds")),
        "product_id",
    )
    .join(
        order_items.groupBy("product_id").agg(count("*").alias("purchases")),
        "product_id",
    )
    .withColumn("cart_conversion", (col("cart_adds") / col("views")) * 100)
    .withColumn("purchase_conversion", (col("purchases") / col("cart_adds")) * 100)
)
conversion_funnel.write.mode("overwrite").parquet(f"{output_path}/conversion_funnel")
