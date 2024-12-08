import uuid
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker
import random
import logging
import time
from typing import Dict, List
import signal
import sys
import os
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()


class ProductViewProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.fake = Faker()
        self.kafka_config = kafka_config
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_sessions = self.load_active_sessions()
        self.valid_product_ids = self.load_valid_product_ids()

    def setup_connections(self, kafka_config: Dict, db_config: Dict):
        self.conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        )

    def setup_kafka_topic(self):
        admin_client = None
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_config["bootstrap_servers"]
            )
            existing_topics = admin_client.list_topics()
            if "product_views" not in existing_topics:
                logger.info("Creating topic 'product_views'...")
                topic = NewTopic(
                    name="product_views", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'product_views' created.")
            else:
                logger.info("Topic 'product_views' already exists.")
        except Exception as e:
            logger.error(f"Error creating topic 'product_views': {e}")
        finally:
            if admin_client:
                admin_client.close()

    def load_active_sessions(self, retry_interval: int = 10) -> List[Dict]:
        """
        Continuously fetch active user sessions. Retries indefinitely if no sessions are available.
        """
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.session_id, s.user_id
                    FROM sessions s
                    INNER JOIN users u ON s.user_id = u.user_id
                    WHERE u.is_active = true
                    """
                )
                if sessions := cur.fetchall():
                    logger.info(f"Loaded {len(sessions)} active sessions")
                    return sessions
                logger.warning("No active sessions found. Retrying...")
                time.sleep(retry_interval)


    def load_valid_product_ids(self, retry_interval: int = 10) -> List[str]:
        """
        Continuously fetch valid product IDs. Retries indefinitely if no products are available.
        """
        while True:
            with self.conn.cursor() as cur:
                cur.execute("SELECT product_id FROM products WHERE is_active = true")
                if products := cur.fetchall():
                    product_ids = [product["product_id"] for product in products]
                    logger.info(f"Loaded {len(product_ids)} valid product IDs")
                    return product_ids
                logger.warning("No valid product IDs found. Retrying...")
                time.sleep(retry_interval)



    def generate_product_view(self) -> Dict:
        """
        Generate a single product view event with realistic patterns.
        """
        if not self.active_sessions:
            raise ValueError("No active sessions available.")
        if not self.valid_product_ids:
            raise ValueError("No valid product IDs available.")

        now = datetime.now()
        session = random.choice(self.active_sessions)
        product_id = random.choice(self.valid_product_ids)

        source_page = random.choices(
            ["homepage", "search", "recommendations", "ads"],
            weights=[50, 30, 15, 5],  # Weighted probabilities
        )[0]

        return {
            "view_id": str(uuid.uuid4()),
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "product_id": product_id,
            "view_timestamp": now.isoformat(),
            "view_duration": random.randint(10, 300),  # View duration in seconds
            "source_page": source_page,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }


    def produce_events(self):
        """
        Continuously produce product view events with realistic traffic patterns.
        """
        try:
            i = 0
            while True:
                event = self.generate_product_view()
                try:
                    self.producer.send("product_views", event)
                    logger.info(f"Produced event: {event['view_id']}")
                except Exception as e:
                    logger.error(f"Error sending event to Kafka: {e}")

                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} product view events.")

                # Simulate user traffic
                time.sleep(random.uniform(0.1, 0.5))
        except Exception as e:
            logger.error(f"Error producing product view events: {e}")
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


    producer = ProductViewProducer(kafka_config, db_config)
    producer.produce_events()
