import faker
import psycopg2
import random
import time
import logging
from datetime import datetime

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the Faker library
fake = faker.Faker()

def generate_transaction():
    """
    Generates a simulated financial transaction.

    Returns:
        dict: A dictionary containing the transaction details.
    """
    user = fake.simple_profile()

    transaction = {
        "transaction_id": fake.uuid4(),
        "user_id": user['username'],
        "timestamp": datetime.utcnow().isoformat(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "city": fake.city(),
        "country": fake.country(),
        "merchant_name": fake.company(),
        "payment_method": random.choice(["credit_card", "debit_card", "bitcoin"]),
        "ip_address": fake.ipv4(),
        "voucher_code": random.choice(["", "DISCOUNT10", ""]),
        "affiliate_id": fake.uuid4()
    }
    logging.info(f"Generated transaction: {transaction}")
    # logging.info(f"Generated transaction: {transaction['transaction_id']}: {transaction['user_id']}")
    return transaction

def create_table(conn):
    """
    Creates the transactions table in the database if it does not exist.

    Args:
        conn (psycopg2.extensions.connection): Database connection.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions(
                transaction_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255),
                timestamp TIMESTAMP,
                amount DECIMAL,
                currency VARCHAR(255),
                city VARCHAR(255),
                country VARCHAR(255),
                merchant_name VARCHAR(255),
                payment_method VARCHAR(255),
                ip_address VARCHAR(255),
                voucher_code VARCHAR(255),
                affiliate_id VARCHAR(255)
            )
        """)
        cursor.close()
        conn.commit()
        logging.info("Table 'transactions' created or already exists.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()

def main():
    """
    Main function to connect to the database, create the table, and insert transactions.
    """
    try:
        conn = psycopg2.connect(
            dbname="financial_db",
            user="postgres",
            password="postgres",
            port=5432,
            host="localhost"
        )
        logging.info("Connected to the database.")
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        exit(1)

    create_table(conn)
    
    for _ in range(50):
        try:
            transaction = generate_transaction()

            cursor = conn.cursor()
            logging.info(f"Inserting transaction: {transaction['transaction_id']}")

            cursor.execute("""
                INSERT INTO transactions(
                    transaction_id,
                    user_id,
                    timestamp,
                    amount,
                    currency,
                    city,
                    country,
                    merchant_name,
                    payment_method,
                    ip_address,
                    voucher_code,
                    affiliate_id
                ) VALUES(
                    %(transaction_id)s,
                    %(user_id)s,
                    %(timestamp)s,
                    %(amount)s,
                    %(currency)s,
                    %(city)s,
                    %(country)s,
                    %(merchant_name)s,
                    %(payment_method)s,
                    %(ip_address)s,
                    %(voucher_code)s,
                    %(affiliate_id)s
                )
            """, transaction)

            cursor.close()
            conn.commit()
            logging.info(f"Transaction {transaction['transaction_id']} inserted successfully.")
        except Exception as e:
            logging.error(f"Error inserting transaction: {e}")
            conn.rollback()
        
        time.sleep(1)

    conn.close()
    logging.info("Database connection closed.")

if __name__ == "__main__":
    main()