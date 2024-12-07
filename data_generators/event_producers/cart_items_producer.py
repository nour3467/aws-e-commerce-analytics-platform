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

    def load_active_carts_and_products(self, batch_size: int = 1000):
        """
        Fetch active carts with valid product references in batches and push to Kafka.

        :param batch_size: Number of rows to fetch in each batch.
        """
        offset = 0
        query = f"""
            SELECT c.cart_id, c.user_id, c.session_id, p.product_id, p.price
            FROM carts c
            INNER JOIN products p ON p.is_active = true
            WHERE c.status = 'active'
            LIMIT {batch_size} OFFSET %s
        """

        while True:
            with self.conn.cursor() as cur:
                cur.execute(query, (offset,))
                batch = cur.fetchall()

                if not batch:
                    logger.info("No more active carts and products found. Stopping.")
                    break  # Exit the loop if no more data is found

                logger.info(
                    f"Fetched {len(batch)} rows from database. Pushing to Kafka."
                )

                # Generate and push events for the batch
                for row in batch:
                    event = self.generate_cart_item_event_from_row(row)
                    self.producer.send("cart_items", event)

                logger.info(f"Pushed {len(batch)} cart item events to Kafka.")
                offset += batch_size  # Increment offset for the next batch
                # wait for a while to simulate a real-world scenario: not all carts are updated at the same time (e.g. 15 s)
                time.sleep(15)

    def generate_cart_item_event_from_row(self, cart_product: Dict) -> Dict:
        """
        Generate a cart item event from a database row.

        :param cart_product: A row containing cart and product data.
        :return: A cart item event dictionary.
        """
        now = datetime.now()

        quantity = random.randint(1, 5)
        unit_price = cart_product["price"]

        return {
            "cart_item_id": str(uuid.uuid4()),
            "cart_id": cart_product["cart_id"],
            "product_id": cart_product["product_id"],
            "quantity": quantity,
            "added_timestamp": now.isoformat(),
            "removed_timestamp": (
                None
                if random.random() > 0.7
                else (now + timedelta(minutes=random.randint(1, 60))).isoformat()
            ),
            "unit_price": unit_price,
        }

    def produce_events(self):
        try:
            logger.info("Starting to produce cart item events...")
            self.load_active_carts_and_products(batch_size=1000)
            logger.info("Finished producing all cart item events.")
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
