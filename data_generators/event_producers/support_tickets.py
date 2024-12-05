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


class SupportTicketProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
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

    def load_active_orders_and_users(self) -> List[Dict]:
        """Fetch completed orders and associated users. Retries if none found."""
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
                time.sleep(10)

    def generate_support_ticket_event(self) -> Dict:
        if not self.active_orders_and_users:
            raise ValueError("No completed orders or users found in the database")

        now = datetime.now()
        order_user = random.choice(self.active_orders_and_users)

        issue_types = ["Delivery Issue", "Payment Issue", "Product Defect", "Other"]
        priorities = ["High", "Medium", "Low"]
        statuses = ["Open", "In Progress", "Resolved"]

        resolved_at = (
            None
            if random.random() > 0.6
            else (now + timedelta(days=random.randint(1, 7))).isoformat()
        )
        satisfaction_score = None if resolved_at is None else random.randint(1, 5)

        return {
            "ticket_id": str(uuid.uuid4()),
            "user_id": order_user["user_id"],
            "order_id": order_user["order_id"],
            "issue_type": random.choice(issue_types),
            "priority": random.choice(priorities),
            "status": random.choice(statuses),
            "created_at": now.isoformat(),
            "resolved_at": resolved_at,
            "satisfaction_score": satisfaction_score,
        }

    def produce_events(self):
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                event = self.generate_support_ticket_event()
                self.producer.send("support_tickets", event)
                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} support ticket events")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate traffic patterns
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
        "dbname": "ecommerce",
        "user": "postgres",
        "password": "admin_password",
        "host": "postgres",
    }

    kafka_config = {"bootstrap_servers": ["kafka:9092"]}

    producer = SupportTicketProducer(kafka_config, db_config)
    producer.produce_events()
