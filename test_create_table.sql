-- Test CREATE TABLE via psql
CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(100),
    email VARCHAR(100)
);

-- Insert some data
INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com');
INSERT INTO customers VALUES (2, 'Jane Smith', 'jane@example.com');

-- Query the data
SELECT * FROM customers;

-- Create another table
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date VARCHAR(20),
    total_amount INT
);

-- Show tables
SELECT * FROM pg_tables;