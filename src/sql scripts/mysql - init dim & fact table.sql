-- 1. Buat Database 
CREATE DATABASE IF NOT EXISTS DWH_DE_asgn_30;
USE DWH_DE_asgn_30;

-- 2. Buat Tabel Dim Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    phone VARCHAR(50),
    state VARCHAR(10),
    last_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 3. Buat Tabel Dim Products
CREATE TABLE IF NOT EXISTS dim_products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    profit_margin DECIMAL(5, 2), -- Menyimpan hasil % margin
    category VARCHAR(50),
    supplier_name VARCHAR(100),
    last_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 4. Buat Tabel Fact Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    id INT PRIMARY KEY,
    customer_id INT,
    total_amount DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP,
    last_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

truncate table dim_customers;
truncate table dim_products;
truncate table fact_orders;

