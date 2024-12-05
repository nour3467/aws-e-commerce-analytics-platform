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

    def load_active_carts_and_users(self) -> List[Dict]:
        """Fetch active carts with valid user references. Retries if none found."""
        while True:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT c.cart_id, c.user_id, u.billing_address_id, u.shipping_address_id
                    FROM carts c
                    INNER JOIN users u ON c.user_id = u.user_id
                    WHERE c.status = 'active'
                """)
                if data := cur.fetchall():
                    logger.info(f"Loaded {len(data)} active carts and users")
                    return data
                logger.warning("No active carts and users found. Retrying in 10 seconds...")
                time.sleep(10)

    def generate_order_event(self) -> Dict:
        if not self.active_carts_and_users:
            raise ValueError("No active carts or users found in the database")

        now = datetime.now()
        cart_user = random.choice(self.active_carts_and_users)

        total_amount = random.uniform(50, 500)  # Random order amount
        tax_amount = total_amount * 0.1  # 10% tax
        shipping_amount = random.uniform(5, 20)  # Random shipping cost
        discount_amount = random.uniform(0, 50)  # Random discount

        return {
            'order_id': str(uuid.uuid4()),
            'user_id': cart_user['user_id'],
            'cart_id': cart_user['cart_id'],
            'status': random.choice(['completed', 'pending', 'cancelled']),
            'total_amount': round(total_amount, 2),
            'tax_amount': round(tax_amount, 2),
            'shipping_amount': round(shipping_amount, 2),
            'discount_amount': round(discount_amount, 2),
            'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer']),
            'delivery_method': random.choice(['standard', 'express']),
            'billing_address_id': cart_user['billing_address_id'],
            'shipping_address_id': cart_user['shipping_address_id'],
            'created_at': now.isoformat(),
            'updated_at': now.isoformat()
        }

    def produce_events(self):
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                event = self.generate_order_event()
                self.producer.send('orders', event)
                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} order events")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate user traffic patterns
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
        'bootstrap_servers': ['kafka:9092']
    }

    producer = OrderProducer(kafka_config, db_config)
    producer.produce_events()