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

ALTER TABLE promotions ADD PRIMARY KEY (promo_id);

CREATE TABLE IF NOT EXISTS skus (
    sku VARCHAR(100),
    last_update_time TIMESTAMP,
    site_id VARCHAR(100),
    product_type VARCHAR(100),
    call_promo_service VARCHAR(100)
);

ALTER TABLE skus ADD PRIMARY KEY (sku);


CREATE TABLE IF NOT EXISTS sku_promo_relations (
    institutional_promo_id VARCHAR(100),
    sku_promotions VARCHAR(100),
    sequence INTEGER
);

ALTER TABLE sku_promo_relations ADD CONSTRAINT inst_promo_id_foreign_key 
FOREIGN KEY (institutional_promo_id) 
REFERENCES skus (sku) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE sku_promo_relations ADD CONSTRAINT promo_id_foreign_key 
FOREIGN KEY (sku_promotions) 
REFERENCES promotions (promo_id) ON DELETE CASCADE ON UPDATE CASCADE;

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

--PARA DEFINIR EL TAMAÑO DE MEMORIA QUE POSTGRESS PUEDE USAR DURANTE LA EJECUCIÓN DE VACCUM
O DE COPY Y QUE SE HAGA MAS RÁPIDO
SHOW maintenance_work_mem;
SET maintenance_work_mem = '1GB';

SELECT constraint_name, constraint_type, table_name, column_name, foreign_table_name, foreign_column_name
FROM information_schema.table_constraints
WHERE constraint_type = 'FOREIGN KEY' AND table_name = 'nombre_tabla';

*/