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
import os
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

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

    def load_active_users_and_products(self, batch_size: int = 100, retry_interval: int = 10) -> List[Dict]:
        """
        Continuously fetch active users and products in batches to reduce memory usage.
        Retries indefinitely if no data is found.
        """
        offset = 0
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.user_id, p.product_id
                    FROM users u
                    CROSS JOIN products p
                    WHERE u.is_active = true AND p.is_active = true
                    LIMIT %s OFFSET %s
                    """,
                    (batch_size, offset),
                )
                if data := cur.fetchall():
                    logger.info(f"Fetched {len(data)} rows of active users and products.")
                    return data
                else:
                    logger.warning("No active users or products found. Retrying...")
                    time.sleep(retry_interval)
                    offset = 0  # Reset offset for continuous retry



    def generate_wishlist_event(self) -> Dict:
        """
        Generate a single wishlist event with realistic patterns.
        """
        if not self.active_users_and_products:
            self.active_users_and_products = self.load_active_users_and_products()

        now = datetime.now()
        user_product = random.choice(self.active_users_and_products)

        # Simulate a realistic likelihood of removal
        removed_probability = 0.2  # 20% chance of removal

        return {
            'wishlist_id': str(uuid.uuid4()),
            'user_id': user_product['user_id'],
            'product_id': user_product['product_id'],
            'added_timestamp': now.isoformat(),
            'removed_timestamp': None if random.random() > removed_probability else (now + timedelta(minutes=random.randint(10, 300))).isoformat(),
            'notes': random.choices(
                ["Favorite item", "Gift idea", "Will buy later", "On sale"],
                weights=[40, 30, 20, 10],  # Weighted probabilities for notes
            )[0],
        }


    def produce_events(self):
        """
        Continuously produce wishlist events with controlled traffic patterns.
        """
        try:
            i = 0
            while True:
                try:
                    event = self.generate_wishlist_event()
                    self.producer.send('wishlists', event)
                    logger.info(f"Produced event: {event['wishlist_id']}")
                except Exception as e:
                    logger.error(f"Error sending wishlist event to Kafka: {e}")

                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} wishlist events.")

                # Mimic user traffic with controlled delays
                time.sleep(random.uniform(0.2, 1.0))  # Increased delay range
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
        "dbname": os.getenv("DB_NAME", "ecommerce"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "admin_password"),
        "host": os.getenv("DB_HOST", "localhost"),
    }

    kafka_config = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
    }

    producer = WishlistProducer(kafka_config, db_config)
    producer.produce_events()
