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
import os
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if os.getenv("AWS_EXECUTION_ENV") is None:
    load_dotenv()
    logger.info("Running locally, loading environment variables from .env")
else:
    logger.info("Running on AWS, using ECS-injected environment variables")


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

    def load_active_tickets(self, retry_interval: int = 10) -> List[Dict]:
        """
        Continuously fetch active support tickets. Retries indefinitely if no tickets are found.
        """
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticket_id
                    FROM support_tickets
                    WHERE status IN ('Open', 'In Progress')
                    """
                )
                if tickets := cur.fetchall():
                    logger.info(f"Loaded {len(tickets)} active support tickets")
                    return tickets
                logger.warning("No active support tickets found. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)


    def generate_ticket_message_event(self) -> Dict:
        """
        Generate a single ticket message event with realistic patterns.
        """
        if not self.active_tickets:
            self.active_tickets = self.load_active_tickets()

        now = datetime.now()
        ticket = random.choice(self.active_tickets)

        sender_type = random.choice(['Customer', 'Support Agent'])
        message_texts = {
            'Customer': [
                "Can you update me on this?",
                "I need more details.",
                "Is there any progress on my issue?",
                "Thanks for your help so far!"
            ],
            'Support Agent': [
                "We are working on your issue.",
                "Thanks for your patience!",
                "Could you provide more details?",
                "Your issue has been resolved."
            ]
        }

        response_delay = (
            random.uniform(0.5, 2.0) if sender_type == 'Support Agent' else random.uniform(5.0, 10.0)
        )  # Faster for agents, slower for customers

        time.sleep(response_delay)  # Simulate conversation flow

        return {
            'message_id': str(uuid.uuid4()),
            'ticket_id': ticket['ticket_id'],
            'sender_type': sender_type,
            'message_text': random.choice(message_texts[sender_type]),
            'created_at': now.isoformat()
        }


    def produce_events(self):
        """
        Continuously produce ticket message events with realistic traffic patterns.
        """
        try:
            i = 0
            while True:
                try:
                    event = self.generate_ticket_message_event()
                    self.producer.send('ticket_messages', event)
                    logger.info(f"Produced event: {event['message_id']}")
                except Exception as e:
                    logger.error(f"Error sending event to Kafka: {e}")

                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} ticket message events.")

                # Mimic real-world conversation delays
                time.sleep(random.uniform(0.1, 1.0))
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
        "dbname": os.getenv("DB_NAME", "ecommerce"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "admin_password"),
        "host": os.getenv("DB_HOST", "localhost"),
    }

    kafka_config = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
    }

    producer = TicketMessageProducer(kafka_config, db_config)
    producer.produce_events()
