import uuid
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker
import random
import logging
import argparse
import time
from typing import Dict, List
import signal
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ProductViewProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.fake = Faker()
        self.kafka_config = kafka_config
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_sessions = self.load_active_sessions()

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
            logger.error(f"Error creating topic 'product_viewsxx': {e}")
        finally:
            if admin_client:
                admin_client.close()

    def load_active_sessions(self) -> List[Dict]:
        """Fetch active sessions with valid user and session references. Retries if none found."""
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
                logger.warning("No active sessions found. Retrying in 10 seconds...")
                time.sleep(10)

    def generate_product_view(self) -> Dict:
        if not self.active_sessions:
            raise ValueError("No active sessions found in database")

        now = datetime.now()
        session = random.choice(self.active_sessions)

        return {
            "view_id": str(uuid.uuid4()),
            "session_id": session["session_id"],
            "user_id": session["user_id"],
            "product_id": str(
                uuid.uuid4()
            ),  # Mock product_id; replace with actual logic if needed
            "view_timestamp": now.isoformat(),
            "view_duration": random.randint(1, 300),  # View duration in seconds
            "source_page": random.choice(
                ["homepage", "search", "recommendations", "ads"]
            ),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

    def produce_events(self):
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                event = self.generate_product_view()
                self.producer.send("product_views", event)
                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} product view events")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate user traffic patterns
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
        "dbname": "ecommerce",
        "user": "postgres",
        "password": "admin_password",
        "host": "postgres",
    }

    kafka_config = {"bootstrap_servers": ["kafka:9092"]}

    producer = ProductViewProducer(kafka_config, db_config)
    producer.produce_events()
