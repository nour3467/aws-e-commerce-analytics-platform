import uuid
import json
from datetime import datetime
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


class OrderItemProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_orders_and_products = self.load_active_orders_and_products()

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

            if "order_items" not in existing_topics:
                logger.info("Creating topic 'order_items'...")
                topic = NewTopic(
                    name="order_items", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'order_items' created.")
            else:
                logger.info("Topic 'order_items' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'order_items': {e}")

        finally:
            if admin_client:
                admin_client.close()


    def load_active_orders_and_products(self) -> List[Dict]:
        """Fetch active orders and valid product references. Retries if none found."""
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.order_id, p.product_id, p.price
                    FROM orders o
                    INNER JOIN products p ON p.is_active = true
                    WHERE o.status = 'completed'
                    """
                )
                if data := cur.fetchall():
                    logger.info(f"Loaded {len(data)} active orders and products")
                    return data
                logger.warning("No active orders and products found. Retrying in 10 seconds...")
                time.sleep(10)

    def generate_order_item_event(self) -> Dict:
        if not self.active_orders_and_products:
            raise ValueError("No active orders or products found in the database")

        now = datetime.now()
        order_product = random.choice(self.active_orders_and_products)

        quantity = random.randint(1, 5)
        unit_price = order_product["price"]
        discount_amount = random.uniform(
            0, unit_price * quantity * 0.2
        )  # Max 20% discount

        return {
            "order_item_id": str(uuid.uuid4()),
            "order_id": order_product["order_id"],
            "product_id": order_product["product_id"],
            "quantity": quantity,
            "unit_price": round(unit_price, 2),
            "discount_amount": round(discount_amount, 2),
            "created_at": now.isoformat(),
        }

    def produce_events(self):
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                event = self.generate_order_item_event()
                self.producer.send("order_items", event)
                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} order item events")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate traffic
        except Exception as e:
            logger.error(f"Error producing order item events: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        if self.producer:
            self.producer.close()
        if self.conn:
            self.conn.close()


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

    kafka_config = {"bootstrap_servers": ["kafka:9092"]}

    producer = OrderItemProducer(kafka_config, db_config)
    producer.produce_events()
