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

    def load_active_sessions_and_users(self, batch_size: int = 100, retry_interval: int = 30):
        """
        Continuously fetch active sessions and users in batches. Waits and retries if no data is available.
        """
        offset = 0
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT s.session_id, u.user_id
                    FROM sessions s
                    INNER JOIN users u ON s.user_id = u.user_id
                    WHERE u.is_active = true
                    LIMIT %s OFFSET %s
                    """,
                    (batch_size, offset),
                )
                batch = cur.fetchall()

                if not batch:
                    logger.warning("No active sessions found. Retrying...")
                    time.sleep(retry_interval)
                    continue

                logger.info(f"Fetched {len(batch)} active sessions.")
                yield batch  # Yield batch for event generation

                offset += batch_size



    def generate_cart_event(self, session_user: Dict) -> Dict:
        """
        Generate a single cart event with real-world patterns using session_user data.
        """
        now = datetime.now()

        # Simulate realistic cart activity
        cart_status = random.choices(
            ["active", "abandoned", "completed"], weights=[60, 30, 10]
        )[0]

        # Generate cart creation times
        created_at = now - timedelta(minutes=random.randint(1, 60))
        updated_at = (
            created_at + timedelta(minutes=random.randint(1, 30))
            if cart_status != "active"
            else now
        )

        return {
            "cart_id": str(uuid.uuid4()),
            "user_id": session_user["user_id"],
            "session_id": session_user["session_id"],
            "status": cart_status,
            "created_at": created_at,
            "updated_at": updated_at,
        }


    def produce_events(self):
        """
        Continuously produce cart events with realistic traffic patterns.
        """
        try:
            logger.info("Starting to produce cart events...")
            for batch in self.load_active_sessions_and_users(batch_size=100):
                for session_user in batch:
                    try:
                        event = self.generate_cart_event(session_user)
                        self.producer.send("carts", value=event)
                        logger.info(f"Produced event: {event['cart_id']}")
                    except Exception as e:
                        logger.error(f"Error sending event to Kafka: {e}")
                time.sleep(random.uniform(1, 5))  # Mimic traffic delay
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
