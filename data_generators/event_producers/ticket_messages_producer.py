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

class TicketMessageProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.kafka_config = kafka_config
        self.setup_connections(kafka_config, db_config)
        self.setup_kafka_topic()
        self.active_tickets = self.load_active_tickets()

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

            if "ticket_messages" not in existing_topics:
                logger.info("Creating topic 'ticket_messages'...")
                topic = NewTopic(
                    name="ticket_messages", num_partitions=3, replication_factor=1
                )
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'ticket_messages' created.")
            else:
                logger.info("Topic 'ticket_messages' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'ticket_messages': {e}")
        finally:
            if admin_client:
                admin_client.close()

    def load_active_tickets(self) -> List[Dict]:
        """Fetch active support tickets. Retries if none found."""
        while True:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT ticket_id
                    FROM support_tickets
                    WHERE status IN ('Open', 'In Progress')
                """)
                if data := cur.fetchall():
                    logger.info(f"Loaded {len(data)} active support tickets")
                    return data
                logger.warning("No active support tickets found. Retrying in 10 seconds...")
                time.sleep(10)

    def generate_ticket_message_event(self) -> Dict:
        if not self.active_tickets:
            raise ValueError("No active tickets found in the database")

        now = datetime.now()
        ticket = random.choice(self.active_tickets)

        sender_types = ['Customer', 'Support Agent']
        message_texts = [
            "Can you update me on this?",
            "We are working on your issue.",
            "Thanks for your patience!",
            "Could you provide more details?",
            "Your issue has been resolved."
        ]

        return {
            'message_id': str(uuid.uuid4()),
            'ticket_id': ticket['ticket_id'],
            'sender_type': random.choice(sender_types),
            'message_text': random.choice(message_texts),
            'created_at': now.isoformat()
        }

    def produce_events(self):
        try:
            i = 0
            while True:  # Infinite loop for real-world mimic
                event = self.generate_ticket_message_event()
                self.producer.send('ticket_messages', event)
                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} ticket message events")
                time.sleep(random.uniform(0.1, 0.5))  # Simulate message activity
        except Exception as e:
            logger.error(f"Error producing ticket message events: {e}")
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

    producer = TicketMessageProducer(kafka_config, db_config)
    producer.produce_events()
