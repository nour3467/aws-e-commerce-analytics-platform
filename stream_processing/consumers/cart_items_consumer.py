import json
import psycopg2
from kafka import KafkaConsumer


class CartItemsConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "cart_items",  # Topic name
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
                CREATE TABLE IF NOT EXISTS cart_items (
                    cart_item_id UUID PRIMARY KEY,
                    cart_id UUID NOT NULL,
                    product_id UUID NOT NULL,
                    quantity INTEGER NOT NULL,
                    added_timestamp TIMESTAMP NOT NULL,
                    removed_timestamp TIMESTAMP,
                    unit_price DECIMAL(10, 2) NOT NULL
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
                cart_item = message.value
                print(f"Received cart item: {cart_item}")

                # Insert into the database
                self.cur.execute(
                    """
                    INSERT INTO cart_items (cart_item_id, cart_id, product_id, quantity, added_timestamp, removed_timestamp, unit_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cart_item_id) DO NOTHING
                    """,
                    (
                        cart_item["cart_item_id"],
                        cart_item["cart_id"],
                        cart_item["product_id"],
                        cart_item["quantity"],
                        cart_item["added_timestamp"],
                        cart_item["removed_timestamp"],
                        cart_item["unit_price"],
                    ),
                )
                self.conn.commit()
                print(f"Saved cart item {cart_item['cart_item_id']} to the database.")

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
    consumer = CartItemsConsumer()
    consumer.run()
