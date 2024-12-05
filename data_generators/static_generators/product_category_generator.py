import uuid
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Dict, List
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProductCategoryGenerator:
    def __init__(self, db_config: Dict):
        self.conn = psycopg2.connect(**db_config)
        # Realistic e-commerce category hierarchy
        self.categories_hierarchy = {
            "Electronics & Computers": {
                "description": "Latest technology and gadgets",
                "subcategories": {
                    "Smartphones & Accessories": "Mobile phones and accessories",
                    "Laptops & Computers": "Computing solutions for work and gaming",
                    "TV & Home Theater": "Entertainment systems and accessories",
                    "Cameras & Photography": "Digital cameras and photography gear",
                    "Wearable Technology": "Smartwatches and fitness trackers",
                    "Gaming & Consoles": "Gaming systems and accessories",
                },
            },
            "Clothing & Fashion": {
                "description": "Trending fashion and apparel",
                "subcategories": {
                    "Men's Fashion": "Men's clothing and accessories",
                    "Women's Fashion": "Women's clothing and accessories",
                    "Kids' Fashion": "Children's clothing and accessories",
                    "Shoes & Footwear": "All types of footwear",
                    "Watches & Jewelry": "Accessories and jewelry items",
                    "Bags & Luggage": "Travel and fashion bags",
                },
            },
            "Home & Kitchen": {
                "description": "Everything for your home",
                "subcategories": {
                    "Furniture": "Home and office furniture",
                    "Kitchen & Dining": "Kitchen appliances and dining essentials",
                    "Home Decor": "Decorative items and artwork",
                    "Bedding & Bath": "Bed and bath essentials",
                    "Storage & Organization": "Home organization solutions",
                    "Garden & Outdoor": "Outdoor furniture and gardening",
                },
            },
            "Sports & Outdoors": {
                "description": "Sports gear and outdoor equipment",
                "subcategories": {
                    "Exercise & Fitness": "Fitness equipment and accessories",
                    "Outdoor Recreation": "Camping and hiking gear",
                    "Sports Equipment": "Equipment for various sports",
                    "Athletic Clothing": "Sports apparel and footwear",
                    "Cycling": "Bikes and cycling accessories",
                    "Water Sports": "Swimming and water sports gear",
                },
            },
        }

    def generate_categories(self) -> List[Dict]:
        """Generate structured category hierarchy"""
        categories = []
        now = datetime.now()

        # Generate main categories
        for main_cat, data in tqdm(
            self.categories_hierarchy.items(), desc="Generating categories"
        ):
            main_cat_id = str(uuid.uuid4())
            categories.append(
                {
                    "category_id": main_cat_id,
                    "parent_category_id": None,
                    "name": main_cat,
                    "description": data["description"],
                    "created_at": now,
                    "updated_at": now,
                }
            )

            # Generate subcategories
            categories.extend(
                {
                    "category_id": str(uuid.uuid4()),
                    "parent_category_id": main_cat_id,
                    "name": sub_cat,
                    "description": sub_desc,
                    "created_at": now,
                    "updated_at": now,
                }
                for sub_cat, sub_desc in data["subcategories"].items()
            )
        return categories

    def batch_insert(self, categories: List[Dict]):
        """Batch insert categories"""
        try:
            with self.conn.cursor() as cur:
                insert_query = """
                    INSERT INTO product_categories
                    (category_id, parent_category_id, name, description, created_at, updated_at)
                    VALUES %s
                    ON CONFLICT (category_id) DO NOTHING
                """
                values = [
                    (
                        cat["category_id"],
                        cat["parent_category_id"],
                        cat["name"],
                        cat["description"],
                        cat["created_at"],
                        cat["updated_at"],
                    )
                    for cat in categories
                ]

                for chunk in tqdm(
                    [values[i : i + 100] for i in range(0, len(values), 100)],
                    desc="Inserting categories",
                ):
                    execute_values(cur, insert_query, chunk)
                self.conn.commit()
                logger.info(f"Successfully inserted {len(categories)} categories")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting categories: {str(e)}")
            raise

    def verify_categories(self):
        """Verify the category hierarchy"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        p1.name as main_category,
                        p2.name as subcategory,
                        p2.description
                    FROM product_categories p1
                    LEFT JOIN product_categories p2
                        ON p2.parent_category_id = p1.category_id
                    WHERE p1.parent_category_id IS NULL
                    ORDER BY p1.name, p2.name
                """
                )
                for row in cur.fetchall():
                    logger.info(f"{row[0]} -> {row[1]}: {row[2]}")
        except Exception as e:
            logger.error(f"Error verifying categories: {str(e)}")
            raise


def main():
    db_config = {
        "dbname": "ecommerce",
        "user": "postgres",
        "password": "admin_password",
        "host": "localhost",
        "port": "5432",
    }

    generator = ProductCategoryGenerator(db_config)
    try:
        categories = generator.generate_categories()
        generator.batch_insert(categories)
        generator.verify_categories()
    finally:
        generator.conn.close()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
