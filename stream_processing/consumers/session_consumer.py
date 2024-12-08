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


class SessionsConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "sessions",  # Topic name
                bootstrap_servers=kafka_config["bootstrap_servers"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            # PostgreSQL Connection Setup
            self.conn = psycopg2.connect(**db_config)
            self.cur = self.conn.cursor()

            # Ensure Table Exists
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id UUID PRIMARY KEY,
                    user_id UUID NOT NULL,
                    timestamp_start TIMESTAMP NOT NULL,
                    timestamp_end TIMESTAMP NOT NULL,
                    device_type VARCHAR(50),
                    os_info VARCHAR(50),
                    browser_info VARCHAR(50),
                    ip_address VARCHAR(50),
                    referral_source VARCHAR(50),
                    utm_source VARCHAR(50),
                    utm_medium VARCHAR(50),
                    utm_campaign VARCHAR(255),
                    created_at TIMESTAMP NOT NULL
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
                session = message.value
                print(f"Received session: {session}")

                # Insert into the database
                self.cur.execute(
                    """
                    INSERT INTO sessions (session_id, user_id, timestamp_start, timestamp_end, device_type, os_info, browser_info, ip_address, referral_source, utm_source, utm_medium, utm_campaign, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO NOTHING
                    """,
                    (
                        session["session_id"],
                        session["user_id"],
                        session["timestamp_start"],
                        session["timestamp_end"],
                        session["device_type"],
                        session["os_info"],
                        session["browser_info"],
                        session["ip_address"],
                        session["referral_source"],
                        session["utm_source"],
                        session["utm_medium"],
                        session["utm_campaign"],
                        session["created_at"],
                    ),
                )
                self.conn.commit()
                print(f"Saved session {session['session_id']} to the database.")

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
    consumer = SessionsConsumer()
    consumer.run()
