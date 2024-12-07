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


class SessionProducer:
    def __init__(self, kafka_config: Dict, db_config: Dict):
        self.fake = Faker()
        self.kafka_config = kafka_config
        self.db_config = db_config
        self.setup_connections()
        self.setup_kafka_topic()
        self.active_users = self.load_active_users()

        self.device_patterns = {
            "mobile": {
                "os": ["iOS 16", "iOS 17", "Android 13", "Android 14"],
                "browser": ["Mobile Safari", "Chrome Mobile", "Samsung Internet"],
                "weight": 0.65,
            },
            "desktop": {
                "os": ["Windows 11", "macOS 13", "macOS 14", "Ubuntu 22.04"],
                "browser": ["Chrome", "Firefox", "Safari", "Edge"],
                "weight": 0.25,
            },
            "tablet": {
                "os": ["iPadOS 16", "iPadOS 17", "Android 13"],
                "browser": ["Mobile Safari", "Chrome", "Samsung Internet"],
                "weight": 0.10,
            },
        }

        self.traffic_sources = {
            "organic_search": {"source": "google", "medium": "organic", "weight": 0.35},
            "paid_search": {"source": "google_ads", "medium": "cpc", "weight": 0.25},
            "social": {
                "source": ["facebook", "instagram", "twitter"],
                "medium": "social",
                "weight": 0.20,
            },
            "email": {"source": "newsletter", "medium": "email", "weight": 0.15},
            "direct": {"source": None, "medium": None, "weight": 0.05},
        }

    def setup_connections(self):
        self.conn = psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config["bootstrap_servers"],
            value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        )

    def setup_kafka_topic(self):
        """Ensure the Kafka topic exists, create it if it doesn't."""

        admin_client = None

        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_config["bootstrap_servers"])
            existing_topics = admin_client.list_topics()

            if "sessions" not in existing_topics:
                logger.info("Creating topic 'sessions'...")
                topic = NewTopic(name="sessions", num_partitions=3, replication_factor=1)
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.info("Topic 'sessions' created.")
            else:
                logger.info("Topic 'sessions' already exists.")

        except Exception as e:
            logger.error(f"Error creating topic 'sessions': {e}")
        finally:
            if admin_client:
                admin_client.close()


    def load_active_users(self, retry_interval: int = 10) -> List[str]:
        """
        Continuously fetch active users. Retries indefinitely if no users are available.
        """
        while True:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id
                    FROM users
                    WHERE is_active = true
                    """
                )
                users = [row["user_id"] for row in cur.fetchall()]
                if users:
                    logger.info(f"Loaded {len(users)} active users")
                    return users
                logger.warning("No active users found. Retrying...")
                time.sleep(retry_interval)


    def get_device_info(self) -> Dict[str, str]:
        devices = list(self.device_patterns.keys())
        weights = [p["weight"] for p in self.device_patterns.values()]
        device_type = random.choices(devices, weights=weights)[0]
        pattern = self.device_patterns[device_type]

        return {
            "device_type": device_type,
            "os_info": random.choice(pattern["os"]),
            "browser_info": random.choice(pattern["browser"]),
        }

    def get_traffic_source(self) -> Dict[str, str]:
        source_type = random.choices(
            list(self.traffic_sources.keys()),
            weights=[s["weight"] for s in self.traffic_sources.values()],
        )[0]

        source_data = self.traffic_sources[source_type]
        source = (
            random.choice(source_data["source"])
            if isinstance(source_data["source"], list)
            else source_data["source"]
        )

        campaign = None
        if source_type in ["paid_search", "email"]:
            campaign = (
                f"{source_type}_{self.fake.slug()}_{datetime.now().strftime('%Y%m')}"
            )

        return {
            "referral_source": source,
            "utm_source": source,
            "utm_medium": source_data["medium"],
            "utm_campaign": campaign,
        }

    def generate_session(self) -> Dict:
        """
        Generate a single session event with realistic patterns.
        """
        if not self.active_users:
            self.active_users = self.load_active_users()

        now = datetime.now()

        # Adjust session duration based on device type
        device_info = self.get_device_info()
        session_duration = random.randint(
            5, 30
        ) if device_info["device_type"] == "mobile" else random.randint(15, 120)

        traffic_info = self.get_traffic_source()

        return {
            "session_id": str(uuid.uuid4()),
            "user_id": random.choice(self.active_users),
            "timestamp_start": now.isoformat(),
            "timestamp_end": (now + timedelta(minutes=session_duration)).isoformat(),
            "device_type": device_info["device_type"],
            "os_info": device_info["os_info"],
            "browser_info": device_info["browser_info"],
            "ip_address": self.fake.ipv4(),
            "referral_source": traffic_info["referral_source"],
            "utm_source": traffic_info["utm_source"],
            "utm_medium": traffic_info["utm_medium"],
            "utm_campaign": traffic_info["utm_campaign"],
            "created_at": now.isoformat(),
        }


    def produce_events(self):
        """
        Continuously produce session events with realistic traffic patterns.
        """
        try:
            i = 0
            while True:
                try:
                    event = self.generate_session()
                    self.producer.send("sessions", event)
                    logger.info(f"Produced event: {event['session_id']}")
                except Exception as e:
                    logger.error(f"Error sending session to Kafka: {e}")

                i += 1
                if i % 100 == 0:
                    logger.info(f"Produced {i} session events.")

                # Mimic user traffic with peak patterns
                current_hour = datetime.now().hour
                if 18 <= current_hour <= 22:  # Peak hours
                    time.sleep(random.uniform(0.1, 0.3))
                else:  # Off-peak hours
                    time.sleep(random.uniform(0.3, 0.8))
        except Exception as e:
            logger.error(f"Error producing session events: {e}")
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
        # "host": "localhost",
    }

    kafka_config = {"bootstrap_servers": ["kafka:29092"]}

    producer = SessionProducer(kafka_config, db_config)
    producer.produce_events()
