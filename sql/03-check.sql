SHOW CATALOGS;
SELECT * FROM lakekeeper.lakehouse.people ORDER BY id;
SELECT * FROM silver.lakehouse.dim_store ORDER BY store_id, valid_from;
SELECT * FROM gold.lakehouse.sales_by_store ORDER BY store_name;
