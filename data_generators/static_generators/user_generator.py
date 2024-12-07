import uuid
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import logging
from faker import Faker
from typing import Dict, List
import random
import hashlib
import json
import sys
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserGenerator:
    def __init__(self, db_config: Dict):
        self.conn = psycopg2.connect(**db_config)
        self.fake = Faker()

        # User preferences options
        self.preferences_options = {
            "language": ["en", "es", "fr", "de"],
            "currency": ["USD", "EUR", "GBP"],
            "theme": ["light", "dark"],
            "email_notifications": [True, False],
            "marketing_preferences": {
                "promotions": [True, False],
                "newsletters": [True, False],
                "product_updates": [True, False],
            },
        }

    def generate_password_hash(self, password: str) -> str:
        """Generate a password hash"""
        return hashlib.sha256(password.encode()).hexdigest()

    def generate_user_preferences(self) -> Dict:
        """Generate random user preferences"""
        return {
            "language": random.choice(self.preferences_options["language"]),
            "currency": random.choice(self.preferences_options["currency"]),
            "theme": random.choice(self.preferences_options["theme"]),
            "email_notifications": random.choice(
                self.preferences_options["email_notifications"]
            ),
            "marketing": {
                "promotions": random.choice(
                    self.preferences_options["marketing_preferences"]["promotions"]
                ),
                "newsletters": random.choice(
                    self.preferences_options["marketing_preferences"]["newsletters"]
                ),
                "product_updates": random.choice(
                    self.preferences_options["marketing_preferences"]["product_updates"]
                ),
            },
        }

    def generate_registration_date(self) -> datetime:
        """Generate a realistic registration date within the last 2 years"""
        return self.fake.date_time_between(
            start_date="-2y", end_date="now", tzinfo=None
        )

    def generate_users(self, num_users: int = 1000) -> List[Dict]:
        """Generate specified number of users"""
        users = []

        for _ in tqdm(range(num_users), desc="Generating users"):
            registration_date = self.generate_registration_date()
            last_login = (
                registration_date
                + timedelta(
                    days=random.randint(0, (datetime.now() - registration_date).days)
                )
                if random.random() > 0.1
                else None
            )  # 10% never logged in

            user = {
                "user_id": str(uuid.uuid4()),
                "email": self.fake.unique.email(),
                "password_hash": self.generate_password_hash(self.fake.password()),
                "first_name": self.fake.first_name(),
                "last_name": self.fake.last_name(),
                "registration_date": registration_date,
                "last_login": last_login,
                "is_active": random.random() > 0.05,  # 95% active users
                "preferences": json.dumps(self.generate_user_preferences()),
                "created_at": registration_date,
                "updated_at": last_login if last_login else registration_date,
            }
            users.append(user)

        return users

    def batch_insert(self, users: List[Dict], batch_size: int = 1000):
        """Batch insert users"""
        try:
            with self.conn.cursor() as cur:
                insert_query = """
                    INSERT INTO users (
                        user_id, email, password_hash, first_name, last_name,
                        registration_date, last_login, is_active, preferences,
                        created_at, updated_at
                    ) VALUES %s
                """
                for i in tqdm(range(0, len(users), batch_size), desc="Inserting users"):
                    batch = users[i : i + batch_size]
                    values = [
                        (
                            user["user_id"],
                            user["email"],
                            user["password_hash"],
                            user["first_name"],
                            user["last_name"],
                            user["registration_date"],
                            user["last_login"],
                            user["is_active"],
                            user["preferences"],
                            user["created_at"],
                            user["updated_at"],
                        )
                        for user in batch
                    ]

                    execute_values(cur, insert_query, values)
                    self.conn.commit()
                    logger.info(f"Inserted batch {i // batch_size + 1}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting users: {str(e)}")
            raise
        finally:
            self.conn.close()

    def verify_users(self):
        """Verify user data distribution"""
        try:
            with self.conn.cursor() as cur:
                # Check user statistics
                cur.execute(
                    """
                    SELECT
                        COUNT(*) as total_users,
                        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) as active_users,
                        SUM(CASE WHEN last_login IS NOT NULL THEN 1 ELSE 0 END) as logged_in_users,
                        MIN(registration_date) as earliest_registration,
                        MAX(registration_date) as latest_registration
                    FROM users;
                """
                )
                stats = cur.fetchone()

                logger.info("\nUser Statistics:")
                logger.info(f"Total Users: {stats[0]}")
                logger.info(
                    f"Active Users: {stats[1]} ({(stats[1]/stats[0]*100):.1f}%)"
                )
                logger.info(
                    f"Users who logged in: {stats[2]} ({(stats[2]/stats[0]*100):.1f}%)"
                )
                logger.info(f"Registration Period: {stats[3]} to {stats[4]}")
        except Exception as e:
            logger.error(f"Error verifying users: {str(e)}")
            raise


def main():
    db_config = {
        "dbname": "ecommerce",
        "user": "postgres",
        "password": "admin_password",
        "host": "localhost",
    }

    try:
        num_users = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
        generator = UserGenerator(db_config)
        users = generator.generate_users(num_users)
        generator.batch_insert(users)
        generator.verify_users()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("Process completed.")


if __name__ == "__main__":
    main()
