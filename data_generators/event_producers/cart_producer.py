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
        self.kafka_config = kafka_config
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.setup_carts_table()  # Ensure the `carts` table exists
        self.active_sessions_and_users = self.load_active_sessions_and_users()

    def setup_connections(self, kafka_config: Dict, db_config: Dict):
        self.conn = psycopg2.connect(**db_config, cursor_factory=RealDictCursor)
        self.cur = self.conn.cursor()  # Create a cursor for database operations
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_config["bootstrap_servers"],
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        )

    def setup_carts_table(self):
        """Ensure the `carts` table exists in the database."""
        try:
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS carts (
                    cart_id UUID PRIMARY KEY,
                    user_id UUID NOT NULL,
                    session_id UUID NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
            self.conn.commit()
            logger.info("Ensured `carts` table exists in the database.")
        except Exception as e:
            logger.error(f"Error ensuring `carts` table exists: {e}")

    def setup_kafka_topic(self):
        """Ensure the Kafka topic exists, create it if it doesn't."""
        admin_client = None

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.producer.config["bootstrap_servers"]
            )
            existing_topics = admin_client.list_topics()

            if "carts" not in existing_topics:
                logger.info("Creating topic 'carts'...")
                topic = NewTopic(name="carts", num_partitions=3, replication_factor=1)
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'carts' created.")
            else:
                logger.info("Topic 'carts' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'carts': {e}")

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
                logger.warning(
                    "No active sessions and users found. Retrying in 10 seconds..."
                )
                time.sleep(10)

    def generate_cart_event(self) -> Dict:
        """Generate a single cart event."""
        if not self.active_sessions_and_users:
            raise ValueError("No active sessions or users found in the database")

        now = datetime.now()
        session_user = random.choice(self.active_sessions_and_users)

        return {
            "cart_id": str(uuid.uuid4()),
            "user_id": session_user["user_id"],
            "session_id": session_user["session_id"],
            "status": random.choice(["active", "abandoned", "completed"]),
            "created_at": now,
            "updated_at": now,  # Reflect the schema
        }

    def insert_cart_event_to_db(self, event: Dict):
        """Insert a cart event into the database."""
        try:
            self.cur.execute(
                """
                INSERT INTO carts (cart_id, user_id, session_id, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (cart_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    event["cart_id"],
                    event["user_id"],
                    event["session_id"],
                    event["status"],
                    event["created_at"],
                    event["updated_at"],
                ),
            )
            self.conn.commit()
            logger.info(
                f"Inserted or updated cart event {event['cart_id']} into the database."
            )
        except Exception as e:
            logger.error(f"Error inserting cart event into the database: {e}")

    def produce_events(self):
        """Generate and process cart events."""
        try:
            i = 0
            while True:
                event = self.generate_cart_event()

                # Produce to Kafka
                self.producer.send("carts", event)

                # Insert into the database
                self.insert_cart_event_to_db(event)

                i += 1
                if i % 100 == 0:
                    logger.info(f"Processed {i} cart events.")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate user traffic patterns

        except Exception as e:
            logger.error(f"Error producing cart events: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources."""
        if self.producer:
            self.producer.close()
        if self.conn:
            self.conn.close()
        logger.info("Closed all resources.")


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

    producer = CartEventProducer(kafka_config, db_config)
    producer.produce_events()
