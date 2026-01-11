-- 1. Buat Schema
CREATE SCHEMA IF NOT EXISTS raw_data;

-- 2. Buat Tabel Suppliers
CREATE TABLE IF NOT EXISTS raw_data.suppliers (
    id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL
);

-- 3. Buat Tabel Products
CREATE TABLE IF NOT EXISTS raw_data.products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    category VARCHAR(50),
    supplier_id INT REFERENCES raw_data.suppliers(id),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Buat Tabel Customers
CREATE TABLE IF NOT EXISTS raw_data.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    phone VARCHAR(50),
    state VARCHAR(10),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Buat Tabel Orders
CREATE TABLE IF NOT EXISTS raw_data.orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES raw_data.customers(id),
    total_amount DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --- SEEDING DUMMY DATA ---

-- Insert Suppliers
INSERT INTO raw_data.suppliers (supplier_name) VALUES 
('Mega Tech Inc'), 
('Global Imports');

-- Insert Products
-- Note: Product ID 2 sengaja dibuat Price 0 untuk test ZeroDivisionError
INSERT INTO raw_data.products (name, price, cost, category, supplier_id, updated_at) VALUES
('Laptop Gaming', 1500.00, 1200.00, 'electronics', 1, NOW()),
('Free Sample Widget', 0.00, 5.00, 'marketing', 2, NOW()), 
('Office Chair', 200.00, 150.00, 'furniture', 2, NOW());

-- Insert Customers
-- Note: 
-- ID 1: Data bersih
-- ID 2: Phone kotor (ada huruf) & State lowercase (untuk test Uppercase transform)
INSERT INTO raw_data.customers (name, phone, state, updated_at) VALUES
('Budi Santoso', '(021) 555-1234', 'DKI', NOW()),
('Siti Aminah', '0812-abc-9999', 'jabar', NOW()); 

-- Insert Orders
-- Note:
-- ID 2: Amount negatif (untuk test validation logic -> jadi 0)
-- ID 1: Status mixed case (PeNDing -> pending)
INSERT INTO raw_data.orders (customer_id, total_amount, status, created_at, updated_at) VALUES
(1, 1500.00, 'PeNDing', NOW(), NOW()),
(2, -500.00, 'FAILED', NOW(), NOW()),
(1, 200.00, 'completed', NOW(), NOW());