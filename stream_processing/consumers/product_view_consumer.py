import json
import psycopg2
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load .env only if not running in AWS
if os.getenv("AWS_EXECUTION_ENV") is None:
    load_dotenv()
    logger.info("Running locally, loading environment variables from .env")
else:
    logger.info("Running on AWS, using ECS-injected environment variables")

# Configuration from environment variables
db_config = {
    "dbname": os.getenv("DB_NAME", "ecommerce"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin_password"),
    "host": os.getenv("DB_HOST", "postgres"),
}

kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(
        ","
    ),
}


class ProductViewsConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "product_views",  # Topic name
                bootstrap_servers=kafka_config["bootstrap_servers"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            # PostgreSQL Connection Setup
            self.conn = psycopg2.connect(**db_config)
            self.cur = self.conn.cursor()

            # Ensure Table Exists
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS product_views (
                    view_id UUID PRIMARY KEY,
                    session_id UUID REFERENCES sessions(session_id),
                    product_id UUID REFERENCES products(product_id),
                    view_timestamp TIMESTAMP DEFAULT now(),
                    view_duration INTERVAL,
                    source_page VARCHAR(255),
                    CONSTRAINT product_views_session_view_timestamp_idx UNIQUE (session_id, view_timestamp)
                );
                """
            )
            self.conn.commit()
            print("Connected to the database and ensured table exists.")

        except Exception as e:
            print(f"Error during initialization: {e}")
            self.cleanup()

    def run(self):
        try:
            print("Starting to consume messages from Kafka...")
            for message in self.consumer:
                try:
                    product_view = message.value
                    print(f"Received product view: {product_view}")

                    # Insert into the database
                    self.cur.execute(
                        """
                        INSERT INTO product_views (view_id, session_id, product_id, view_timestamp, view_duration, source_page)
                        VALUES (%s, %s, %s, %s, %s::INTERVAL, %s)
                        ON CONFLICT (view_id) DO NOTHING
                        """,
                        (
                            product_view["view_id"],
                            product_view["session_id"],
                            product_view["product_id"],
                            product_view["view_timestamp"],
                            f"{product_view['view_duration']} seconds",  # Cast integer to interval
                            product_view["source_page"],
                        ),
                    )
                    self.conn.commit()
                    print(
                        f"Saved product view {product_view['view_id']} to the database."
                    )
                except Exception as e:
                    print(f"Error processing message {message.value}: {e}")
                    self.conn.rollback()  # Rollback in case of failure for individual message

        except Exception as e:
            print(f"Error while consuming messages: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        # Ensure proper closure of resources
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error during cleanup: {e}")


if __name__ == "__main__":
    consumer = ProductViewsConsumer()
    consumer.run()
