import json
import psycopg2
from kafka import KafkaConsumer


class SessionsConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "sessions",  # Topic name
                bootstrap_servers=["kafka:29092"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )

            # PostgreSQL Connection Setup
            self.conn = psycopg2.connect(
                dbname="ecommerce",
                user="postgres",
                password="admin_password",
                host="postgres",  # Use host.docker.internal if necessary
            )
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
