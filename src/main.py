import faker
import psycopg2
import random
from datetime import datetime

fake = faker.Faker('pt_BR')

def generate_transaction():
    user = fake.simple_profile()

    return{
        "transaction_id": fake.uuid4(),
        "user_id": user['username'],
        "timestamp": datetime.utcnow().timestamp(),
        "amount": round(random.randint(10, 1000),2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "city": fake.city(),
        "country": fake.country(),
        "merchant_name": fake.company(),
        "payment_method": random.choice(["credit_card", "debit_cart", "bitcoin"]),
        "ip_address": fake.ipv4(),
        "voucher_code": random.choice(["", "DISCOUNT10", ""]),
        "affiliate_id":fake.uuid4()
    }

def cretae_table(conn):
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

if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname="financial_db",
        user="postgres",
        password="postgres",
        port=5432,
        host="localhost"
    )

    cretae_table(conn)
    
    transaction = generate_transaction()

    cursor = conn.cursor()
    print(transaction)

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
            to_timestamp(%(timestamp)s),
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
    conn.close()