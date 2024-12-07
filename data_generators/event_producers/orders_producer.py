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
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.kafka_config = kafka_config
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_carts_and_users = self.load_active_carts_and_users()

    def setup_connections(self, kafka_config: Dict, db_config: Dict):
        self.conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
        )

    def setup_kafka_topic(self):
        """Ensure the Kafka topic exists, create it if it doesn't."""

        admin_client = None

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_config["bootstrap_servers"]
            )
            existing_topics = admin_client.list_topics()

            if "orders" not in existing_topics:
                logger.info("Creating topic 'orders'...")
                topic = NewTopic(
                    name="orders", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'orders' created.")
            else:
                logger.info("Topic 'orders' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'orders': {e}")

        finally:
            if admin_client:
                admin_client.close()

    def load_active_carts_and_users(self, batch_size: int = 100, retry_interval: int = 10) -> List[Dict]:
        """
        Continuously fetch active carts with valid user references and their addresses.
        Waits and retries indefinitely if no data is available.
        """
        offset = 0
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        c.cart_id,
                        c.user_id,
                        ba.address_id AS billing_address_id,
                        sa.address_id AS shipping_address_id
                    FROM carts c
                    INNER JOIN users u ON c.user_id = u.user_id
                    LEFT JOIN user_addresses ba ON u.user_id = ba.user_id AND ba.address_type = 'billing' AND ba.is_default = true
                    LEFT JOIN user_addresses sa ON u.user_id = sa.user_id AND sa.address_type = 'shipping' AND sa.is_default = true
                    WHERE c.status = 'active'
                    LIMIT %s OFFSET %s
                    """,
                    (batch_size, offset),
                )
                batch = cur.fetchall()

                if batch:
                    logger.info(f"Fetched {len(batch)} active carts and users with addresses.")
                    return batch
                else:
                    logger.warning("No active carts found. Retrying in 10 seconds...")
                    time.sleep(retry_interval)




    def generate_order_event(self) -> Dict:
        """
        Generate a single order event with realistic patterns.
        """
        if not self.active_carts_and_users:
            raise ValueError("No active carts or users found in the database")

        now = datetime.now()
        cart_user = random.choice(self.active_carts_and_users)

        total_amount = random.uniform(50, 500)
        tax_amount = total_amount * 0.1
        shipping_amount = random.uniform(5, 20)
        discount_amount = random.uniform(0, min(50, total_amount * 0.2))  # Max 20% discount

        status = random.choices(
            ["completed", "pending", "cancelled"],
            weights=[70, 20, 10],  # 70% completed, 20% pending, 10% cancelled
        )[0]

        return {
            "order_id": str(uuid.uuid4()),
            "user_id": cart_user["user_id"],
            "cart_id": cart_user["cart_id"],
            "status": status,
            "total_amount": round(total_amount, 2),
            "tax_amount": round(tax_amount, 2),
            "shipping_amount": round(shipping_amount, 2),
            "discount_amount": round(discount_amount, 2),
            "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
            "delivery_method": random.choice(["standard", "express"]),
            "billing_address_id": cart_user["billing_address_id"],
            "shipping_address_id": cart_user["shipping_address_id"],
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }


    def produce_events(self):
        """
        Continuously produce order events with realistic traffic patterns.
        """
        try:
            i = 0
            while True:
                try:
                    event = self.generate_order_event()
                    self.producer.send("orders", event)
                    logger.info(f"Produced event: {event['order_id']}")
                except Exception as e:
                    logger.error(f"Error sending event to Kafka: {e}")

                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} order events.")

                # Mimic user traffic
                time.sleep(random.uniform(0.1, 0.5))  # Simulate realistic delays

        except Exception as e:
            logger.error(f"Error producing order events: {e}")
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

    kafka_config = {
        'bootstrap_servers': ['kafka:29092']
    }

    producer = OrderProducer(kafka_config, db_config)
    producer.produce_events()
