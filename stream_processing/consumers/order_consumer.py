import json
import psycopg2
from kafka import KafkaConsumer


class OrdersConsumer:
    def __init__(self):
        try:
            # Kafka Consumer Setup
            self.consumer = KafkaConsumer(
                "orders",  # Topic name
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
                CREATE TABLE IF NOT EXISTS orders (
                    order_id UUID PRIMARY KEY,
                    user_id UUID NOT NULL,
                    cart_id UUID NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    total_amount DECIMAL(10, 2) NOT NULL,
                    tax_amount DECIMAL(10, 2) NOT NULL,
                    shipping_amount DECIMAL(10, 2),
                    discount_amount DECIMAL(10, 2),
                    payment_method VARCHAR(50),
                    delivery_method VARCHAR(50),
                    billing_address_id UUID,
                    shipping_address_id UUID,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
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
                order = message.value
                print(f"Received order: {order}")

                # Insert into the database
                self.cur.execute(
                    """
                    INSERT INTO orders (order_id, user_id, cart_id, status, total_amount, tax_amount, shipping_amount, discount_amount, payment_method, delivery_method, billing_address_id, shipping_address_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        total_amount = EXCLUDED.total_amount,
                        tax_amount = EXCLUDED.tax_amount,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        order["order_id"],
                        order["user_id"],
                        order["cart_id"],
                        order["status"],
                        order["total_amount"],
                        order["tax_amount"],
                        order["shipping_amount"],
                        order["discount_amount"],
                        order["payment_method"],
                        order["delivery_method"],
                        order["billing_address_id"],
                        order["shipping_address_id"],
                        order["created_at"],
                        order["updated_at"],
                    ),
                )
                self.conn.commit()
                print(f"Saved order {order['order_id']} to the database.")

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
    consumer = OrdersConsumer()
    consumer.run()
