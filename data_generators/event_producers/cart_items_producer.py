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
from typing import Dict, List
import signal
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CartItemProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.setup_connections(kafka_config, db_config)
        self.kafka_config = kafka_config
        self.setup_kafka_topic()

    def setup_connections(self, kafka_config: Dict, db_config: Dict):
        self.conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        )

    def setup_kafka_topic(self):
        """Ensure the Kafka topic exists, create it if it doesn't."""
        admin_client = None

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_config["bootstrap_servers"]
            )
            existing_topics = admin_client.list_topics()

            if "cart_items" not in existing_topics:
                logger.info("Creating topic 'cart_items'...")
                topic = NewTopic(
                    name="cart_items", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'cart_items' created.")
            else:
                logger.info("Topic 'cart_items' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'cart_items': {e}")

        finally:
            if admin_client:
                admin_client.close()

    def load_active_carts_and_products(self, batch_size: int = 100, retry_interval: int = 30):
        """
        Continuously fetch active carts and products in batches. Waits and retries if no data is available.
        """
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
                    logger.warning("No active carts and products found. Retrying...")
                    time.sleep(retry_interval)
                    continue

                logger.info(f"Fetched {len(batch)} rows from database. Pushing to Kafka.")
                yield batch  # Yield batch to produce events

                offset += batch_size



    def generate_cart_item_event_from_row(self, cart_product: Dict) -> Dict:
        """
        Generate a cart item event from a database row.

        :param cart_product: A row containing cart and product data.
        :return: A cart item event dictionary.
        """
        now = datetime.now()

        # Randomized quantities to simulate user behavior
        quantity = random.randint(1, 5)
        unit_price = cart_product["price"]

        # Simulating user activity: random item removal
        removed_probability = random.random()  # Real-world pattern: 30% chance of removal
        removed_timestamp = (
            None
            if removed_probability > 0.3  # Retain item in cart 70% of the time
            else (now + timedelta(minutes=random.randint(1, 60))).isoformat()
        )

        return {
            "cart_item_id": str(uuid.uuid4()),
            "cart_id": cart_product["cart_id"],
            "product_id": cart_product["product_id"],
            "quantity": quantity,
            "added_timestamp": now.isoformat(),
            "removed_timestamp": removed_timestamp,
            "unit_price": unit_price,
        }


    def produce_events(self):
        """
        Continuously produce cart item events with realistic traffic patterns.
        """
        try:
            logger.info("Starting to produce cart item events...")
            for batch in self.load_active_carts_and_products(batch_size=100):
                for row in batch:
                    event = self.generate_cart_item_event_from_row(row)
                    try:
                        self.producer.send("cart_items", event)
                        logger.info(f"Produced event: {event['cart_item_id']}")
                    except Exception as e:
                        logger.error(f"Error sending event to Kafka: {e}")
                time.sleep(random.uniform(5, 15))  # Mimic user activity delay

        except Exception as e:
            logger.error(f"Error producing cart item events: {e}")
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
        logger.info("Graceful shutdown initiated...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    db_config = {
        "dbname": "ecommerce",
        "user": "postgres",
        "password": "admin_password",
        "host": "postgres",
    }

    kafka_config = {"bootstrap_servers": ["kafka:29092"]}

    producer = CartItemProducer(kafka_config, db_config)
    producer.produce_events()
