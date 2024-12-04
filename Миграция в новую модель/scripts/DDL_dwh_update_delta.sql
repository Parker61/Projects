 -- делаем выборку заказчиков, по которым были изменения в DWH. По этим заказчикам данные в витрине нужно будет обновить
DROP TABLE IF EXISTS dwh.dwh_update_delta;

CREATE TABLE dwh_update_delta AS (
    SELECT     
            dd.exist_customer_id AS customer_id
            FROM dwh_delta dd 
                WHERE dd.exist_customer_id IS NOT NULL        
);