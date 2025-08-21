from pathlib import Path
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": "5000",
    "dbname": "db",
    "user": "agri_db",
    "password": "alexa"
}


def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.OperationalError as e:
        raise
    except psycopg2.Error as e:
        raise


def create_tables(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;

            CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_descriptions TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT
            );

            CREATE TABLE IF NOT EXISTS dev.customers (
                id SERIAL PRIMARY KEY,
                firstname TEXT,
                lastname TEXT,
                email TEXT,
                phone TEXT,
                birthday DATE,
                gender TEXT,
                address TEXT,
                zip TEXT,
                city TEXT
            );

            CREATE TABLE IF NOT EXISTS dev.products (
                id SERIAL PRIMARY KEY,
                title TEXT,
                price FLOAT,
                description TEXT,
                category TEXT
            );

            CREATE TABLE IF NOT EXISTS dev.orders (
                id SERIAL PRIMARY KEY,
                order_id INT,
                customer_id INT,
                product_id INT,
                quantity INT,
                date DATE,
                city TEXT
            );
            """)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise


def insert_data(conn):

    try:
        with conn.cursor() as cursor:
            # Insert weather data
            cursor.execute("""
                INSERT INTO dev.raw_weather_data (city, temperature, weather_descriptions, wind_speed, time, utc_offset)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, ("Nairobi", 24.5, "Sunny", 10.5, datetime.utcnow(), "+03:00"))

            # Insert a customer
            cursor.execute("""
                INSERT INTO dev.customers (firstname, lastname, email, phone, birthday, gender, address, zip, city)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, ("John", "Doe", "john@example.com", "0712345678", "1990-05-12", "Male", "123 Street", "00100", "Nairobi"))

            # Insert a product
            cursor.execute("""
                INSERT INTO dev.products (title, price, description, category)
                VALUES (%s, %s, %s, %s)
            """, ("Maize Seeds", 500.0, "High quality maize seeds", "Agriculture"))

            # Insert an order
            cursor.execute("""
                INSERT INTO dev.orders (order_id, customer_id, product_id, quantity, date, city)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (1001, 1, 1, 2, "2025-08-21", "Nairobi"))

        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        raise


def main():
    try:
        conn = connect_to_db()
        create_tables(conn)
        insert_data(conn)
    except Exception as e:
        pass
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
