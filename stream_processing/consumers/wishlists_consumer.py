import json
import psycopg2
from kafka import KafkaConsumer


class WishlistsConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "wishlists",  # Topic name
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
                CREATE TABLE IF NOT EXISTS wishlists (
                    wishlist_id UUID PRIMARY KEY,
                    user_id UUID NOT NULL,
                    product_id UUID NOT NULL,
                    added_timestamp TIMESTAMP NOT NULL,
                    removed_timestamp TIMESTAMP,
                    notes TEXT
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
                wishlist = message.value
                print(f"Received wishlist: {wishlist}")

                # Insert into the database
                self.cur.execute(
                    """
                    INSERT INTO wishlists (wishlist_id, user_id, product_id, added_timestamp, removed_timestamp, notes)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (wishlist_id) DO NOTHING
                    """,
                    (
                        wishlist["wishlist_id"],
                        wishlist["user_id"],
                        wishlist["product_id"],
                        wishlist["added_timestamp"],
                        wishlist["removed_timestamp"],
                        wishlist["notes"],
                    ),
                )
                self.conn.commit()
                print(f"Saved wishlist {wishlist['wishlist_id']} to the database.")

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
    consumer = WishlistsConsumer()
    consumer.run()
