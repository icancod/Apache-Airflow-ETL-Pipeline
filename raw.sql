USE raw;
CREATE TABLE ecommerce_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    product_id VARCHAR(20),
    product_name VARCHAR(200),
    category VARCHAR(100),
    brand VARCHAR(100),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_pct DECIMAL(5,2),
    order_date DATE,
    shipped_date DATE,
    delivery_date DATE,
    payment_mode VARCHAR(20),
    return_status VARCHAR(20),
    city VARCHAR(100)
);