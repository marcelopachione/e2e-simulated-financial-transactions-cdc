import faker
import psycopg2
import random
import time
import logging
from datetime import datetime
from psycopg2.extras import execute_values

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
    logging.info(f"Generated transaction: {transaction['transaction_id']}: {transaction['user_id']}")
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

def insert_transactions_batch(conn, batch_size):
    """
    Inserts a batch of transactions into the database.

    Args:
        conn (psycopg2.extensions.connection): Database connection.
        batch_size (int): Number of transactions to insert in each batch.
    """
    transactions = [generate_transaction() for _ in range(batch_size)]
    
    # Convert list of dictionaries to list of tuples
    transactions_tuples = [
        (
            t["transaction_id"],
            t["user_id"],
            t["timestamp"],
            t["amount"],
            t["currency"],
            t["city"],
            t["country"],
            t["merchant_name"],
            t["payment_method"],
            t["ip_address"],
            t["voucher_code"],
            t["affiliate_id"]
        ) for t in transactions
    ]
    
    try:
        cursor = conn.cursor()
        logging.info(f"Inserting batch of {batch_size} transactions")

        insert_query = """
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
            ) VALUES %s
        """
        
        execute_values(
            cursor, insert_query, transactions_tuples, template=None, page_size=100
        )

        cursor.close()
        conn.commit()
        logging.info(f"Batch of {batch_size} transactions inserted successfully.")
    except Exception as e:
        logging.error(f"Error inserting batch of transactions: {e}")
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
    
    # Insert transactions in batches of 50 every 3 seconds
    for _ in range(50):
        insert_transactions_batch(conn, batch_size=50)
        time.sleep(3)

    conn.close()
    logging.info("Database connection closed.")

if __name__ == "__main__":
    main()