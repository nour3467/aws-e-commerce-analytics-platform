import uuid
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import random
import logging
import time
from typing import Dict
import signal
import sys
import os
from dotenv import load_dotenv

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


class CartItemProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.kafka_config = kafka_config
        self.db_config = db_config
        self.setup_connections()
        self.setup_kafka_topic()

    def setup_connections(self):
        # Database connection
        self.conn = psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)

        # Kafka producer connection
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        )

    def setup_kafka_topic(self):
        """Ensure the Kafka topic exists, create it if it doesn't."""
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.kafka_config["bootstrap_servers"]
        )
        try:
            if "cart_items" not in admin_client.list_topics():
                logger.info("Creating topic 'cart_items'...")
                topic = NewTopic(
                    name="cart_items", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic])
                logger.info("Topic 'cart_items' created.")
            else:
                logger.info("Topic 'cart_items' already exists.")
        except Exception as e:
            logger.error(f"Error creating Kafka topic: {e}")
        finally:
            admin_client.close()

    def load_active_carts_and_products(
        self, batch_size: int = 100, retry_interval: int = 30
    ):
        """Fetch active carts and products from the database in batches."""
        offset = 0
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.cart_id, c.user_id, c.session_id, p.product_id, p.price
                    FROM carts c
                    INNER JOIN products p ON p.is_active = true
                    WHERE c.status = 'active'
                    LIMIT %s OFFSET %s
                    """,
                    (batch_size, offset),
                )
                batch = cur.fetchall()

                if not batch:
                    logger.warning("No active carts found. Retrying...")
                    time.sleep(retry_interval)
                    continue

                yield batch
                offset += batch_size

    def generate_cart_item_event(self, cart_product: Dict) -> Dict:
        """Generate a cart item event."""
        now = datetime.now()
        quantity = random.randint(1, 5)
        removed = (
            None
            if random.random() > 0.3
            else (now + timedelta(minutes=random.randint(1, 60))).isoformat()
        )

        return {
            "cart_item_id": str(uuid.uuid4()),
            "cart_id": cart_product["cart_id"],
            "product_id": cart_product["product_id"],
            "quantity": quantity,
            "added_timestamp": now.isoformat(),
            "removed_timestamp": removed,
            "unit_price": cart_product["price"],
        }

    def produce_events(self):
        """Produce cart item events."""
        logger.info("Producing cart item events...")
        try:
            for batch in self.load_active_carts_and_products():
                for product in batch:
                    event = self.generate_cart_item_event(product)
                    self.producer.send("cart_items", event)
                    logger.info(f"Produced event: {event['cart_item_id']}")
                time.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"Error producing events: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        if self.producer:
            self.producer.close()
        if self.conn:
            self.conn.close()
        logger.info("Connections closed.")


if __name__ == "__main__":

    def signal_handler(sig, frame):
        logger.info("Graceful shutdown...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Database configuration
    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
    }

    # Kafka configuration
    kafka_config = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS").split(","),
    }

    producer = CartItemProducer(kafka_config, db_config)
    producer.produce_events()
