import psycopg2
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "db",     
    "port": "5432",  
    "dbname": "db",
    "user": "agri_db",
    "password": "alexa"
}

def connect_to_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        logger.info("Connecting to the database...")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Database connection established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Operational error connecting to the database: {e}", exc_info=True)
        raise
    except psycopg2.Error as e:
        logger.error(f"Database error occurred: {e}", exc_info=True)
        raise


def create_tables(conn):
    """Create required tables if they don't exist."""
    logger.info("Creating tables if they do not exist...")
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
        logger.info("Tables created successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error creating tables: {e}", exc_info=True)
        raise


def insert_records(conn):
    """Insert sample records into the database."""
    logger.info("Inserting records into the database...")
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dev.raw_weather_data 
                (city, temperature, weather_descriptions, wind_speed, time, utc_offset)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, ("Nairobi", 24.5, "Sunny", 10.5, datetime.utcnow(), "+03:00"))
            logger.info("Inserted weather data record.")

            cursor.execute("""
                INSERT INTO dev.customers 
                (firstname, lastname, email, phone, birthday, gender, address, zip, city)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, ("John", "Doe", "john@example.com", "0712345678",
                  "1990-05-12", "Male", "123 Street", "00100", "Nairobi"))
            logger.info("Inserted customer record.")

            cursor.execute("""
                INSERT INTO dev.products 
                (title, price, description, category)
                VALUES (%s, %s, %s, %s)
            """, ("Maize Seeds", 500.0, "High quality maize seeds", "Agriculture"))
            logger.info("Inserted product record.")

            cursor.execute("""
                INSERT INTO dev.orders 
                (order_id, customer_id, product_id, quantity, date, city)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (1001, 1, 1, 2, "2025-08-21", "Nairobi"))
            logger.info("Inserted order record.")

        conn.commit()
        logger.info("All records inserted successfully.")
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error inserting records: {e}", exc_info=True)
        raise


def main():
    """Main execution flow for the pipeline."""
    try:
        logger.info("Starting pipeline execution...")
        conn = connect_to_db()
        create_tables(conn)
        insert_records(conn)
        logger.info("Pipeline execute successfully.")
        return "Success: Data inserted"

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise
    finally:
        if "conn" in locals() and conn:
            conn.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
