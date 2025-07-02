-- VibeDB Demo SQL Script
-- Run this with: psql -h localhost -p 5433 -U vibedb -d vibedb -f demo.sql

-- Show existing tables
SELECT * FROM users;
SELECT * FROM products;

-- Insert some users
INSERT INTO users (id, name, email, active) VALUES (1, 'Alice', 'alice@example.com', true);
INSERT INTO users (id, name, email, active) VALUES (2, 'Bob', 'bob@example.com', false);
INSERT INTO users (id, name, email, active) VALUES (3, 'Charlie', 'charlie@example.com', true);

-- Insert some products
INSERT INTO products (id, name, price, in_stock) VALUES (1, 'Laptop', 999, true);
INSERT INTO products (id, name, price, in_stock) VALUES (2, 'Mouse', 25, true);
INSERT INTO products (id, name, price, in_stock) VALUES (3, 'Keyboard', 75, false);

-- Query the data
SELECT * FROM users;
SELECT * FROM products;

-- Filter queries
SELECT name, email FROM users WHERE active = true;
SELECT name, price FROM products WHERE in_stock = true;

-- Ordering (Note: ORDER BY is parsed but not fully implemented in execution)
SELECT * FROM users ORDER BY id DESC;

-- Limit (Note: LIMIT is parsed but not fully implemented in execution)
SELECT * FROM products LIMIT 2;