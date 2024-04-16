-- Database: promotions

-- DROP DATABASE IF EXISTS promotions;

CREATE DATABASE promotions
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'Spanish_Mexico.1252'
    LC_CTYPE = 'Spanish_Mexico.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

CREATE TABLE IF NOT EXISTS promotions (
    promo_id VARCHAR(100),
    promo_type VARCHAR(100),
    sort_score VARCHAR(100),
    bank_name VARCHAR(100),
    card_type VARCHAR(100),
    promo_code VARCHAR(100),
    discount_amount VARCHAR(100),
    months VARCHAR(100),
    minimum_purchase_amount VARCHAR(100),
    included_prefixes TEXT,
    excluded_prefixes TEXT,
    promo_description VARCHAR(100),
    site_id VARCHAR(100),
    best_promo VARCHAR(100),
    med_amount VARCHAR(100),
    discount_adicional VARCHAR(100),
    med_description VARCHAR(100),
    promo_group_id VARCHAR(100),
    id_pmr VARCHAR(100),
    minimum_purchase_unit VARCHAR(100),
    discount_unit VARCHAR(100),
    promo_price VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS skus (
    sku VARCHAR(100),
    last_update_time TIMESTAMP,
    site_id VARCHAR(100),
    product_type VARCHAR(100),
    call_promo_service VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS sku_promo_relations (
    sku VARCHAR(100),
    promo_id VARCHAR(100),
    sequence INTEGER
);

ALTER USER "postgres" WITH PASSWORD 'n0m3l0';

SELECT usename, application_name, client_addr, client_port, backend_start, backend_type
FROM pg_stat_activity
WHERE state = 'active';

/*

DELETE FROM promotions;
vacuum full promotions;
select count(*) from promotions;

DELETE FROM skus;
vacuum full skus;
select count(*) from skus;

DELETE FROM sku_promo_relations;
vacuum full sku_promo_relations;
select count(*) from sku_promo_relations;

*/