import uuid
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging
from faker import Faker
from typing import Dict, List
import random
from tqdm import tqdm
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProductGenerator:
    def __init__(self, db_config: Dict):
        self.conn = psycopg2.connect(**db_config)
        self.fake = Faker()
        self.categories = self.load_categories()
        self.existing_skus = self.load_existing_skus()

        # Product patterns by category
        self.patterns = {
            "Electronics & Computers": {
                "price_range": (100, 2000),
                "cost_ratio": (0.6, 0.8),  # 60-80% of price
                "stock_range": (5, 100),
            },
            "Clothing & Fashion": {
                "price_range": (20, 200),
                "cost_ratio": (0.3, 0.5),
                "stock_range": (10, 200),
            },
            "Home & Kitchen": {
                "price_range": (30, 500),
                "cost_ratio": (0.4, 0.6),
                "stock_range": (15, 150),
            },
            "Sports & Outdoors": {
                "price_range": (25, 400),
                "cost_ratio": (0.4, 0.7),
                "stock_range": (10, 100),
            },
        }

    def load_categories(self) -> List[Dict]:
        """Load existing categories from database"""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT category_id, name, parent_category_id FROM product_categories"
            )
            return cur.fetchall()

    def load_existing_skus(self) -> set:
        """Load existing SKUs from the database"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT sku FROM products")
            return set(row[0] for row in cur.fetchall())

    def generate_sku(self, category_name: str) -> str:
        """Generate unique SKU based on category"""
        prefix = "".join(word[0] for word in category_name.split()[:2]).upper()
        while True:
            sku = f"{prefix}-{uuid.uuid4().hex[:8].upper()}"
            if sku not in self.existing_skus:
                self.existing_skus.add(sku)  # Track it to prevent duplicates
                return sku

    def generate_product_name(self, category_name: str) -> str:
        """Generate realistic product name based on category"""
        brands = {
            "Electronics": ["Samsung", "Sony", "LG", "Apple", "Dell", "HP"],
            "Clothing": ["Nike", "Adidas", "Puma", "Levi's", "H&M"],
            "Home": ["KitchenAid", "Cuisinart", "Dyson", "IKEA"],
            "Sports": ["Nike", "Adidas", "Under Armour", "Wilson", "Spalding"],
        }

        brand = random.choice(brands.get(category_name.split()[0], ["Generic"]))
        product_type = self.fake.word()
        model = self.fake.random_number(3)
        return f"{brand} {product_type} {model}"

    def get_category_pattern(self, category_name: str) -> Dict:
        """Get price and stock patterns for category"""
        return next(
            (pattern for key, pattern in self.patterns.items() if key in category_name),
            {
                "price_range": (10, 100),
                "cost_ratio": (0.4, 0.6),
                "stock_range": (10, 100),
            },
        )

    def generate_products(self, num_products: int = 1000):
        """Generate specified number of products"""
        now = datetime.now()

        for _ in tqdm(range(num_products), desc="Generating products"):
            # Select random category
            category = random.choice(self.categories)
            category_name = category[1]  # name is second column

            # Get patterns for this category
            pattern = self.get_category_pattern(category_name)

            # Generate price and cost
            price = round(random.uniform(*pattern["price_range"]), 2)
            cost = round(price * random.uniform(*pattern["cost_ratio"]), 2)

            yield {
                "product_id": str(uuid.uuid4()),
                "sku": self.generate_sku(category_name),
                "name": self.generate_product_name(category_name),
                "description": self.fake.paragraph(nb_sentences=3),
                "category_id": category[0],  # category_id is first column
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(*pattern["stock_range"]),
                "is_active": random.random() < 0.95,  # 95% active
                "created_at": now,
                "updated_at": now,
            }

    def batch_insert(self, num_products: int, batch_size: int = 1000):
        """Batch insert products"""
        try:
            with self.conn.cursor() as cur:
                insert_query = """
                    INSERT INTO products (
                        product_id, sku, name, description, category_id,
                        price, cost, stock_quantity, is_active,
                        created_at, updated_at
                    ) VALUES %s
                """

                count = 0
                for batch in tqdm(
                    (
                        list(self.generate_products(batch_size))
                        for _ in range(0, num_products, batch_size)
                    ),
                    desc="Inserting products",
                    total=(num_products // batch_size),
                ):
                    values = [
                        (
                            p["product_id"],
                            p["sku"],
                            p["name"],
                            p["description"],
                            p["category_id"],
                            p["price"],
                            p["cost"],
                            p["stock_quantity"],
                            p["is_active"],
                            p["created_at"],
                            p["updated_at"],
                        )
                        for p in batch
                    ]
                    execute_values(cur, insert_query, values)
                    count += len(values)
                    self.conn.commit()
                logger.info(f"Successfully inserted {count} products")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting products: {str(e)}")
            raise

    def verify_products(self):
        """Verify product distribution"""
        try:
            with self.conn.cursor() as cur:
                # Check products per category
                cur.execute(
                    """
                    SELECT c.name, COUNT(p.product_id) as product_count,
                           AVG(p.price) as avg_price,
                           SUM(p.stock_quantity) as total_stock
                    FROM product_categories c
                    LEFT JOIN products p ON p.category_id = c.category_id
                    GROUP BY c.name
                    ORDER BY product_count DESC
                """
                )
                logger.info("\nProduct Distribution:")
                for row in cur.fetchall():
                    logger.info(
                        f"Category: {row[0]:<30} "
                        f"Products: {row[1]:<6} "
                        f"Avg Price: ${row[2]:.2f} "
                        f"Total Stock: {row[3]}"
                    )
        except Exception as e:
            logger.error(f"Error verifying products: {str(e)}")
            raise


def main():
    db_config = {
        "dbname": "ecommerce",
        "user": "postgres",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432",
    }

    try:
        num_products = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
        generator = ProductGenerator(db_config)
        generator.batch_insert(num_products)
        generator.verify_products()
    finally:
        generator.conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
