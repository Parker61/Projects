ALTER TABLE staging.user_order_log ADD COLUMN status VARCHAR(15) not null default 'shipped';
ALTER TABLE mart.f_sales ADD COLUMN status VARCHAR(15) not null default 'shipped';

truncate staging.user_order_log;
truncate mart.d_city;
TRUNCATE TABLE mart.d_customer CASCADE;
TRUNCATE TABLE mart.d_item CASCADE;
truncate mart.f_sales;