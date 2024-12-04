-- первая таблица dwh_delta: определяем, какие данные были изменены в витрине данных или добавлены в DWH 
-- формируем дельту изменений
DROP TABLE IF EXISTS dwh.dwh_delta;
CREATE TABLE IF NOT EXISTS dwh.dwh_delta AS (
    SELECT     
            dcs.customer_id AS customer_id,
            dcs.customer_name AS customer_name,
            dcs.customer_address AS customer_address,
            dcs.customer_birthday AS customer_birthday,
            dcs.customer_email AS customer_email,
            fo.order_id AS order_id,
            dp.product_id AS product_id,
            dp.product_price AS product_price,
            dp.product_type AS product_type,
            dc.craftsman_id AS craftsman_id,                                                                  
            DATE_PART('year', AGE(dcs.customer_birthday)) AS customer_age,
            fo.order_completion_date - fo.order_created_date AS diff_order_date, 
            fo.order_status AS order_status,
            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
            crd.customer_id AS exist_customer_id,
            dcs.load_dttm AS customer_load_dttm,
            dc.load_dttm AS craftsman_load_dttm,
            dp.load_dttm AS products_load_dttm
            FROM dwh.f_order fo 
                INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
                LEFT JOIN dwh.customer_report_datamart crd ON dcs.customer_id = crd.customer_id
                     WHERE  (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dcs.load_dttm> (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
);
