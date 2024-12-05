import uuid
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
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


class CartEventProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_sessions_and_users = self.load_active_sessions_and_users()

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

            if "cart_events" not in existing_topics:
                logger.info("Creating topic 'cart_events'...")
                topic = NewTopic(
                    name="cart_events", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'cart_events' created.")
            else:
                logger.info("Topic 'cart_events' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'cart_events': {e}")

        finally:
            if admin_client:
                admin_client.close()

    def load_active_sessions_and_users(self) -> List[Dict]:
        """Fetch active sessions with valid user references. Retries if none found."""
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.session_id, u.user_id
                    FROM sessions s
                    INNER JOIN users u ON s.user_id = u.user_id
                    WHERE u.is_active = true
                    """
                )
                if data := cur.fetchall():
                    logger.info(f"Loaded {len(data)} active sessions and users")
                    return data
                logger.warning("No active sessions and users found. Retrying in 10 seconds...")
                time.sleep(10)

    def generate_cart_event(self) -> Dict:
        if not self.active_sessions_and_users:
            raise ValueError("No active sessions or users found in the database")

        now = datetime.now()
        session_user = random.choice(self.active_sessions_and_users)

        return {
            "cart_id": str(uuid.uuid4()),
            "user_id": session_user["user_id"],
            "session_id": session_user["session_id"],
            "status": random.choice(["active", "abandoned", "completed"]),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),  # Reflect the schema
        }

    def produce_events(self):
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                event = self.generate_cart_event()
                self.producer.send("cart_events", event)
                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} cart events")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate user traffic patterns
        except Exception as e:
            logger.error(f"Error producing cart events: {e}")
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

    producer = CartEventProducer(kafka_config, db_config)
    producer.produce_events()
