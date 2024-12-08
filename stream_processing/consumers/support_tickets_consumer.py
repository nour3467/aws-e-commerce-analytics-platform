import json
import psycopg2
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load .env only if not running in AWS
if os.getenv("AWS_EXECUTION_ENV") is None:
    load_dotenv()
    logger.info("Running locally, loading environment variables from .env")
else:
    logger.info("Running on AWS, using ECS-injected environment variables")

# Configuration from environment variables
db_config = {
    "dbname": os.getenv("DB_NAME", "ecommerce"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin_password"),
    "host": os.getenv("DB_HOST", "postgres"),
}

kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(
        ","
    ),
}


class SupportTicketsConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "support_tickets",  # Topic name
                bootstrap_servers=kafka_config["bootstrap_servers"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            # PostgreSQL Connection Setup
            self.conn = psycopg2.connect(**db_config)
            self.cur = self.conn.cursor()

            # Ensure Table Exists
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS support_tickets (
                    ticket_id UUID PRIMARY KEY,
                    user_id UUID NOT NULL,
                    order_id UUID,
                    issue_type VARCHAR(100),
                    priority VARCHAR(20),
                    status VARCHAR(50),
                    created_at TIMESTAMP NOT NULL,
                    resolved_at TIMESTAMP,
                    satisfaction_score INTEGER
                )
                """
            )
            self.conn.commit()
            print("Connected to the database and ensured table exists.")

        except Exception as e:
            print(f"Error during initialization: {e}")
            self.cleanup()

    def run(self):
        try:
            print("Starting to consume messages from Kafka...")
            for message in self.consumer:
                ticket = message.value
                print(f"Received support ticket: {ticket}")

                # Insert into the database
                self.cur.execute(
                    """
                    INSERT INTO support_tickets (ticket_id, user_id, order_id, issue_type, priority, status, created_at, resolved_at, satisfaction_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticket_id) DO NOTHING
                    """,
                    (
                        ticket["ticket_id"],
                        ticket["user_id"],
                        ticket["order_id"],
                        ticket["issue_type"],
                        ticket["priority"],
                        ticket["status"],
                        ticket["created_at"],
                        ticket["resolved_at"],
                        ticket["satisfaction_score"],
                    ),
                )
                self.conn.commit()
                print(f"Saved support ticket {ticket['ticket_id']} to the database.")

        except Exception as e:
            print(f"Error while consuming messages: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        # Ensure proper closure of resources
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error during cleanup: {e}")


if __name__ == "__main__":
    consumer = SupportTicketsConsumer()
    consumer.run()
