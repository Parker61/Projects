-- делаем перерасчёт для существующих записей витрин, так как данные обновились за отчётные периоды.
-- Логика похожа на insert, но нужно достать конкретные данные из DWH
dwh_delta_update_result AS (
    SELECT 
            T4.customer_id AS customer_id,
            T4.customer_name AS customer_name,
            T4.customer_address AS customer_address,
            T4.customer_birthday AS customer_birthday,
            T4.customer_email AS customer_email,
            T4.customer_money AS customer_money,
            T4.platform_money AS platform_money,
            T4.count_order AS count_order,
            T4.avg_price_order as avg_price_order,
            T4.median_time_order_completed AS median_time_order_completed,
            T4.product_type as top_product_category,
            T4.craftsman_id as craftsman_id,
            T4.count_order_created AS count_order_created,
            T4.count_order_in_progress AS count_order_in_progress,
            T4.count_order_delivery AS count_order_delivery,
            T4.count_order_done AS count_order_done,
            T4.count_order_not_done AS count_order_not_done,
            T4.report_period AS report_period 
            FROM (
                SELECT -- в этой выборке объединяем две внутренние выборки по расчёту столбцов витрины и применяем оконную функцию для определения самого популярного мастера у заказчика
                        *,
                        RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product,
                        ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY count_craftsman_id DESC) AS rank_craftsman_id
                        FROM (
                            SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки,
                            -- кроме столбца самого популярного мастера у заказчика. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним JOIN
                                T1.customer_id AS customer_id,
                                T1.customer_name AS customer_name,
                                T1.customer_address AS customer_address,
                                T1.customer_birthday AS customer_birthday,
                                T1.customer_email AS customer_email,
                                SUM(T1.product_price) AS customer_money,
                                SUM(T1.product_price) * 0.1 AS platform_money,
                                COUNT(order_id) AS count_order,
                                AVG(T1.product_price) AS avg_price_order,
                                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
                                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
                                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
                                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
                                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                                T1.report_period AS report_period
                                FROM (
                                    SELECT     -- в этой выборке достаём из DWH обновлённые или новые данные по заказчикам, которые уже есть в витрине
                                			dcs.customer_id AS customer_id,
                               				dcs.customer_name AS customer_name,
                                			dcs.customer_address AS customer_address,
                               				dcs.customer_birthday AS customer_birthday,
                                			dcs.customer_email AS customer_email,
                                            fo.order_id AS order_id,
                                            dp.product_id AS product_id,
                                            dp.product_price AS product_price,
                                            dp.product_type AS product_type,
                                            fo.order_completion_date - fo.order_created_date AS diff_order_date,
                                            fo.order_status AS order_status, 
                                            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period
                                            FROM dwh.f_order fo 
                                                INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                                                INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                                                INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
                                                INNER JOIN dwh_update_delta ud ON dcs.customer_id = ud.customer_id
                                ) AS T1
                                     GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
                          			  ) AS T2 
                          			  
                               	 INNER JOIN (
                                    SELECT     -- Эта выборка поможет определить самого популярного мастера у заказчика.
                                    -- Эта выборка не делается в предыдущем запросе, так как нужна другая группировка.
                                    -- Для данных этой выборки можно применить оконную функцию, которая и покажет самого популярного мастера у заказчика                                            dd.customer_id AS customer_id_for_craftsman_id, 
                                            dd.customer_id AS customer_id_for_craftsman_id, 
                                            dd.craftsman_id, -- идентификатор самого популярного мастера у заказчика,
                                            COUNT(dd.craftsman_id) AS count_craftsman_id
                                            FROM dwh_delta AS dd
                                                GROUP BY dd.customer_id, dd.craftsman_id
                                                    ORDER BY count_craftsman_id desc
                                            ) AS T3
                                              ON T2.customer_id = T3.customer_id_for_craftsman_id  
                                 INNER JOIN (
                                    SELECT  -- определить самый популярный товар у мастера ручной работы. 
                                    -- Для данных этой выборки можно применить оконную функцию, которая и покажет самую популярную категорию товаров у мастера
                                            dd.craftsman_id AS craftsman_id_for_product_type, 
                                            dd.product_type, 
                                            COUNT(dd.product_id) AS count_product
                                            FROM dwh_delta AS dd
                                                GROUP BY dd.craftsman_id, dd.product_type
                                                    ORDER BY count_product DESC)
                                             AS T5
                                             ON T3.craftsman_id = T5.craftsman_id_for_product_type                                                 
                ) AS T4
                WHERE T4.rank_craftsman_id = 1 and T4.rank_count_product = 1
                ORDER BY report_period
);