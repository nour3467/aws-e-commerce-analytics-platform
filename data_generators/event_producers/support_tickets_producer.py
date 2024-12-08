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
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

if os.getenv("AWS_EXECUTION_ENV") is None:
    load_dotenv()
    logger.info("Running locally, loading environment variables from .env")
else:
    logger.info("Running on AWS, using ECS-injected environment variables")


class SupportTicketProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.kafka_config = kafka_config
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_orders_and_users = self.load_active_orders_and_users()

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

            if "support_tickets" not in existing_topics:
                logger.info("Creating topic 'support_tickets'...")
                topic = NewTopic(
                    name="support_tickets", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'support_tickets' created.")
            else:
                logger.info("Topic 'support_tickets' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'support_tickets': {e}")
        finally:
            if admin_client:
                admin_client.close()

    def load_active_orders_and_users(self, retry_interval: int = 10) -> List[Dict]:
        """
        Continuously fetch completed orders and associated users. Retries indefinitely if no data is found.
        """
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.order_id, o.user_id
                    FROM orders o
                    INNER JOIN users u ON o.user_id = u.user_id
                    WHERE o.status = 'completed'
                    """
                )
                if data := cur.fetchall():
                    logger.info(f"Loaded {len(data)} completed orders and users")
                    return data
                logger.warning("No completed orders and users found. Retrying in 10 seconds...")
                time.sleep(retry_interval)


    def generate_support_ticket_event(self) -> Dict:
        """
        Generate a single support ticket event with realistic patterns.
        """
        if not self.active_orders_and_users:
            self.active_orders_and_users = self.load_active_orders_and_users()

        now = datetime.now()
        order_user = random.choice(self.active_orders_and_users)

        issue_types = ["Delivery Issue", "Payment Issue", "Product Defect", "Other"]
        issue_weights = [50, 20, 20, 10]  # Weighted probabilities
        priorities = ["High", "Medium", "Low"]
        priority_weights = [30, 50, 20]
        statuses = ["Open", "In Progress", "Resolved"]
        status_weights = [50, 30, 20]

        resolved_at = (
            None
            if random.choices([True, False], weights=[40, 60])[0]
            else (now + timedelta(days=random.randint(1, 7))).isoformat()
        )
        satisfaction_score = None if resolved_at is None else random.randint(1, 5)

        return {
            "ticket_id": str(uuid.uuid4()),
            "user_id": order_user["user_id"],
            "order_id": order_user["order_id"],
            "issue_type": random.choices(issue_types, weights=issue_weights)[0],
            "priority": random.choices(priorities, weights=priority_weights)[0],
            "status": random.choices(statuses, weights=status_weights)[0],
            "created_at": now.isoformat(),
            "resolved_at": resolved_at,
            "satisfaction_score": satisfaction_score,
        }


    def produce_events(self):
        """
        Continuously produce support ticket events with realistic traffic patterns.
        """
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                try:
                    event = self.generate_support_ticket_event()
                    self.producer.send("support_tickets", event)
                    logger.info(f"Produced event: {event['ticket_id']}")
                except Exception as e:
                    logger.error(f"Error sending event to Kafka: {e}")

                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} support ticket events.")

                # Simulate traffic patterns
                time.sleep(random.uniform(0.5, 2.0))
        except Exception as e:
            logger.error(f"Error producing support ticket events: {e}")
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

    producer = SupportTicketProducer(kafka_config, db_config)
    producer.produce_events()
