-- Create the airflow_db database and user for Airflow metadata
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow_db OWNER airflow;

-- Create the dev schema and tables in the db database (used by dbt)
\c db

CREATE SCHEMA IF NOT EXISTS dev;

CREATE TABLE dev.customers (
  id INTEGER PRIMARY KEY,
  firstname TEXT,
  lastname TEXT,
  email TEXT NOT NULL,
  phone TEXT,
  birthday DATE,
  gender TEXT,
  address TEXT,
  zip TEXT,
  city TEXT
);

CREATE TABLE dev.products (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  price NUMERIC,
  description TEXT,
  category TEXT
);

CREATE TABLE dev.orders (
  id INTEGER PRIMARY KEY,
  order_id TEXT NOT NULL,
  customer_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  date DATE,
  city TEXT
);

CREATE TABLE dev.raw_weather_data (
  id INTEGER PRIMARY KEY,
  city TEXT NOT NULL,
  temperature NUMERIC,
  weather_descriptions TEXT,
  wind_speed NUMERIC,
  time TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP,
  utc_offset TEXT
);

-- Optional: Insert sample data for testing
INSERT INTO dev.customers (id, first_name, email) VALUES (1, 'John', 'john@example.com');
INSERT INTO dev.products (id, title, price) VALUES (1, 'Apple', 0.99);
INSERT INTO dev.orders (id, order_id, customer_id, product_id, quantity) VALUES (1, 'ORD001', 1, 1, 2);
INSERT INTO dev.raw_weather_data (id, city, temperature, time) VALUES (1, 'Nairobi', 25.5, CURRENT_TIMESTAMP);