USE reporting;
CREATE TABLE category_sales (
    category VARCHAR(100),
    total_orders INT,
    total_quantity INT,
    total_revenue DECIMAL(15,2)
);

CREATE TABLE city_sales (
    city VARCHAR(100),
    total_orders INT,
    total_revenue DECIMAL(15,2)
);

CREATE TABLE kpi_summary (
    metric_name VARCHAR(100),
    metric_value VARCHAR(100)
);


DESCRIBE raw.ecommerce_orders;
DESCRIBE staging.ecommerce_cleaned;
DESCRIBE reporting.category_sales;
DESCRIBE reporting.city_sales;
DESCRIBE reporting.kpi_summary;

DESCRIBE reporting.category_sales;
DESCRIBE reporting.city_sales;
DESCRIBE reporting.kpi_summary;


SELECT COUNT(*) FROM raw.ecommerce_orders;
SELECT COUNT(*) FROM staging.ecommerce_cleaned;
SELECT * FROM reporting.category_sales;
SELECT * FROM reporting.city_sales;
SELECT * FROM reporting.kpi_summary;

