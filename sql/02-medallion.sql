CREATE SCHEMA bronze.lakehouse;
CREATE SCHEMA silver.lakehouse;
CREATE SCHEMA gold.lakehouse;

CREATE TABLE bronze.lakehouse.raw_stores (
    store_id integer,
    store_name varchar,
    city varchar,
    loaded_at timestamp
);

CREATE TABLE bronze.lakehouse.raw_sales (
    sale_id bigint,
    store_id integer,
    product varchar,
    amount decimal(10,2),
    sale_date date
);

INSERT INTO bronze.lakehouse.raw_stores VALUES
    (1, 'Магазин на Тверской', 'Москва', TIMESTAMP '2026-09-01 09:00:00'),
    (2, 'Магазин на Невском', 'Санкт-Петербург', TIMESTAMP '2026-09-01 09:00:00');

INSERT INTO bronze.lakehouse.raw_sales VALUES
    (1001, 1, 'Ноутбук', 85000.00, DATE '2026-09-02'),
    (1002, 1, 'Мышь', 1200.00, DATE '2026-09-02'),
    (1003, 2, 'Клавиатура', 3500.00, DATE '2026-09-03'),
    (1004, 2, 'Монитор', 20000.00, DATE '2026-09-03');

CREATE TABLE silver.lakehouse.dim_store AS
SELECT
    store_id,
    store_name,
    city,
    loaded_at AS valid_from,
    LEAD(loaded_at) OVER (PARTITION BY store_id ORDER BY loaded_at) AS valid_to,
    LEAD(loaded_at) OVER (PARTITION BY store_id ORDER BY loaded_at) IS NULL AS is_current
FROM bronze.lakehouse.raw_stores;

INSERT INTO bronze.lakehouse.raw_stores VALUES
    (1, 'Магазин на Арбате', 'Москва', TIMESTAMP '2026-09-10 09:00:00');

DROP TABLE silver.lakehouse.dim_store;

CREATE TABLE silver.lakehouse.dim_store AS
SELECT
    store_id,
    store_name,
    city,
    loaded_at AS valid_from,
    LEAD(loaded_at) OVER (PARTITION BY store_id ORDER BY loaded_at) AS valid_to,
    LEAD(loaded_at) OVER (PARTITION BY store_id ORDER BY loaded_at) IS NULL AS is_current
FROM bronze.lakehouse.raw_stores;

CREATE TABLE gold.lakehouse.sales_by_store AS
SELECT
    d.store_name,
    d.city,
    COUNT(*) AS sales_count,
    SUM(s.amount) AS total_amount
FROM bronze.lakehouse.raw_sales s
JOIN silver.lakehouse.dim_store d
    ON s.store_id = d.store_id
   AND d.is_current = true
GROUP BY d.store_name, d.city;

SELECT *
FROM silver.lakehouse.dim_store
ORDER BY store_id, valid_from;

SELECT *
FROM gold.lakehouse.sales_by_store
ORDER BY store_name;
