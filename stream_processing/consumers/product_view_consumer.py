import json
import psycopg2
from kafka import KafkaConsumer


class ProductViewsConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "product_views",  # Topic name
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
                CREATE TABLE IF NOT EXISTS product_views (
                    view_id UUID PRIMARY KEY,
                    session_id UUID REFERENCES sessions(session_id),
                    product_id UUID REFERENCES products(product_id),
                    view_timestamp TIMESTAMP DEFAULT now(),
                    view_duration INTERVAL,
                    source_page VARCHAR(255),
                    CONSTRAINT product_views_session_view_timestamp_idx UNIQUE (session_id, view_timestamp)
                );
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
                try:
                    product_view = message.value
                    print(f"Received product view: {product_view}")

                    # Insert into the database
                    self.cur.execute(
                        """
                        INSERT INTO product_views (view_id, session_id, product_id, view_timestamp, view_duration, source_page)
                        VALUES (%s, %s, %s, %s, %s::INTERVAL, %s)
                        ON CONFLICT (view_id) DO NOTHING
                        """,
                        (
                            product_view["view_id"],
                            product_view["session_id"],
                            product_view["product_id"],
                            product_view["view_timestamp"],
                            f"{product_view['view_duration']} seconds",  # Cast integer to interval
                            product_view["source_page"],
                        ),
                    )
                    self.conn.commit()
                    print(
                        f"Saved product view {product_view['view_id']} to the database."
                    )
                except Exception as e:
                    print(f"Error processing message {message.value}: {e}")
                    self.conn.rollback()  # Rollback in case of failure for individual message

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
    consumer = ProductViewsConsumer()
    consumer.run()
