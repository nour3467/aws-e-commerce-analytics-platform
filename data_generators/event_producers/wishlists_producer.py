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
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WishlistProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.kafka_config = kafka_config
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_users_and_products = self.load_active_users_and_products()

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

            if "wishlists" not in existing_topics:
                logger.info("Creating topic 'wishlists'...")
                topic = NewTopic(
                    name="wishlists", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'wishlists' created.")
            else:
                logger.info("Topic 'wishlists' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'wishlists': {e}")

        finally:
            if admin_client:
                admin_client.close()

    def load_active_users_and_products(self) -> List[Dict]:
        """Fetch active users and products for wishlist generation."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT u.user_id, p.product_id
                FROM users u
                CROSS JOIN products p
                WHERE u.is_active = true AND p.is_active = true
            """)
            data = cur.fetchall()
            logger.info(f"Loaded {len(data)} active users and products")
            return data

    def generate_wishlist_event(self) -> Dict:
        if not self.active_users_and_products:
            raise ValueError("No active users or products found in the database")

        now = datetime.now()
        user_product = random.choice(self.active_users_and_products)

        return {
            'wishlist_id': str(uuid.uuid4()),
            'user_id': user_product['user_id'],
            'product_id': user_product['product_id'],
            'added_timestamp': now.isoformat(),
            'removed_timestamp': None if random.random() > 0.8 else (now + timedelta(minutes=random.randint(10, 300))).isoformat(),
            'notes': random.choice([
                "Favorite item",
                "Gift idea",
                "Will buy later",
                "On sale"
            ])
        }

    def produce_events(self):
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                event = self.generate_wishlist_event()
                self.producer.send('wishlists', event)
                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} wishlist events")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate user traffic patterns
        except Exception as e:
            logger.error(f"Error producing wishlist events: {e}")
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

    kafka_config = {"bootstrap_servers": ["kafka:29092"]}

    producer = WishlistProducer(kafka_config, db_config)
    producer.produce_events()
