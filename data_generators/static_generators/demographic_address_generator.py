import uuid
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import logging
from faker import Faker
from typing import Dict, List
import random
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserDemographicAddressGenerator:
    def __init__(self, db_config: Dict):
        self.conn = psycopg2.connect(**db_config)
        self.fake = Faker()

        # Demographics options
        self.age_ranges = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
        self.income_brackets = [
            "0-25k",
            "25k-50k",
            "50k-75k",
            "75k-100k",
            "100k-150k",
            "150k+",
        ]

        # Load existing users
        self.users = self.load_users()

    def load_users(self) -> List[str]:
        """Load existing user IDs from database"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT DISTINCT user_id FROM users")
            return [row[0] for row in cur.fetchall()]

    def generate_demographics(self) -> List[Dict]:
        """Generate demographics for all users"""
        now = datetime.now()

        return [
            {
                "demographic_id": str(uuid.uuid4()),
                "user_id": user_id,
                "age_range": random.choice(self.age_ranges),
                "gender": random.choice(["M", "F", "Other"]),
                "income_bracket": random.choice(self.income_brackets),
                "occupation": self.fake.job(),
                "created_at": now,
                "updated_at": now,
            }
            for user_id in tqdm(self.users, desc="Generating demographics")
        ]

    def generate_addresses(self) -> List[Dict]:
        """Generate addresses for users"""
        addresses = []
        now = datetime.now()

        for user_id in tqdm(self.users, desc="Generating addresses"):
            # Generate 1-3 addresses per user
            num_addresses = random.randint(1, 3)

            for i in range(num_addresses):
                address_type = "billing" if i == 0 else "shipping"
                is_default = i == 0  # First address is default

                addresses.append(
                    {
                        "address_id": str(uuid.uuid4()),
                        "user_id": user_id,
                        "address_type": address_type,
                        "street_address": self.fake.street_address(),
                        "city": self.fake.city(),
                        "state": self.fake.state(),
                        "country": self.fake.country(),
                        "postal_code": self.fake.postcode(),
                        "is_default": is_default,
                        "created_at": now,
                        "updated_at": now,
                    }
                )

        return addresses

    def batch_insert_demographics(
        self, demographics: List[Dict], batch_size: int = 1000
    ):
        """Batch insert demographics"""
        try:
            with self.conn.cursor() as cur:
                insert_query = """
                    INSERT INTO user_demographics (
                        demographic_id, user_id, age_range, gender,
                        income_bracket, occupation, created_at, updated_at
                    ) VALUES %s
                """
                for i in tqdm(
                    range(0, len(demographics), batch_size),
                    desc="Inserting demographics",
                ):
                    batch = demographics[i : i + batch_size]
                    values = [
                        (
                            d["demographic_id"],
                            d["user_id"],
                            d["age_range"],
                            d["gender"],
                            d["income_bracket"],
                            d["occupation"],
                            d["created_at"],
                            d["updated_at"],
                        )
                        for d in batch
                    ]

                    execute_values(cur, insert_query, values)
                    self.conn.commit()
                    logger.info(f"Inserted batch {i // batch_size + 1}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting demographics: {str(e)}")
            raise

    def batch_insert_addresses(self, addresses: List[Dict], batch_size: int = 1000):
        """Batch insert addresses"""
        try:
            with self.conn.cursor() as cur:
                insert_query = """
                    INSERT INTO user_addresses (
                        address_id, user_id, address_type, street_address,
                        city, state, country, postal_code, is_default,
                        created_at, updated_at
                    ) VALUES %s
                """
                for i in tqdm(
                    range(0, len(addresses), batch_size), desc="Inserting addresses"
                ):
                    batch = addresses[i : i + batch_size]
                    values = [
                        (
                            a["address_id"],
                            a["user_id"],
                            a["address_type"],
                            a["street_address"],
                            a["city"],
                            a["state"],
                            a["country"],
                            a["postal_code"],
                            a["is_default"],
                            a["created_at"],
                            a["updated_at"],
                        )
                        for a in batch
                    ]

                    execute_values(cur, insert_query, values)
                    self.conn.commit()
                    logger.info(f"Inserted batch {i // batch_size + 1}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting addresses: {str(e)}")
            raise

    def verify_data(self):
        """Verify generated data"""
        with self.conn.cursor() as cur:
            # Check demographics distribution
            cur.execute(
                """
                SELECT
                    age_range,
                    COUNT(*) as count,
                    ROUND(COUNT(*)::decimal / SUM(COUNT(*)) OVER() * 100, 2) as percentage
                FROM user_demographics
                GROUP BY age_range
                ORDER BY age_range;
            """
            )
            logger.info("\nDemographics Distribution:")
            for row in cur.fetchall():
                logger.info(f"Age Range: {row[0]:<6} Count: {row[1]:<5} ({row[2]}%)")

            # Check address distribution
            cur.execute(
                """
                SELECT
                    address_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT user_id) as unique_users
                FROM user_addresses
                GROUP BY address_type;
            """
            )
            logger.info("\nAddress Distribution:")
            for row in cur.fetchall():
                logger.info(
                    f"Type: {row[0]:<8} Count: {row[1]:<5} Unique Users: {row[2]}"
                )

    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    db_config = {
        "dbname": "ecommerce",
        "user": "postgres",
        "password": "admin_password",
        "host": "localhost",
    }

    generator = UserDemographicAddressGenerator(db_config)

    try:
        # Generate and insert demographics
        demographics = generator.generate_demographics()
        generator.batch_insert_demographics(demographics)

        # Generate and insert addresses
        addresses = generator.generate_addresses()
        generator.batch_insert_addresses(addresses)

        # Verify the data
        generator.verify_data()

    finally:
        generator.close()


if __name__ == "__main__":
    main()
