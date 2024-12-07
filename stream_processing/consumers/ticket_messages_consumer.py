import json
import psycopg2
from kafka import KafkaConsumer


class TicketMessagesConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "ticket_messages",  # Topic name
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
                CREATE TABLE IF NOT EXISTS ticket_messages (
                    message_id UUID PRIMARY KEY,
                    ticket_id UUID NOT NULL,
                    sender_type VARCHAR(50),
                    message_text TEXT,
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
                ticket_message = message.value
                print(f"Received ticket message: {ticket_message}")

                # Insert into the database
                self.cur.execute(
                    """
                    INSERT INTO ticket_messages (message_id, ticket_id, sender_type, message_text, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    (
                        ticket_message["message_id"],
                        ticket_message["ticket_id"],
                        ticket_message["sender_type"],
                        ticket_message["message_text"],
                        ticket_message["created_at"],
                    ),
                )
                self.conn.commit()
                print(
                    f"Saved ticket message {ticket_message['message_id']} to the database."
                )

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
    consumer = TicketMessagesConsumer()
    consumer.run()
