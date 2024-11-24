-- Удаление старых данных
DELETE FROM mart.f_customer_retention 
WHERE f_customer_retention.period_id = (
    SELECT substr(d_calendar.week_of_year_iso, 1, 8) 
    FROM mart.d_calendar 
    WHERE d_calendar.date_actual = '{{ ds }}'
);

WITH 
new_customers AS (
    SELECT  
        customer_id,
        period_name,
        item_id,
        SUM(payment_amount) AS payment_amount
    FROM  
        mart.f_sales
    WHERE 
        status = 'shipped'
    GROUP BY  
        customer_id
    HAVING 
        COUNT(*) = 1  
),

returning_customers AS (
    SELECT  
        customer_id,
        period_name,
        item_id,
        SUM(payment_amount) AS payment_amount
    FROM  
        mart.f_sales
    WHERE 
        status = 'shipped'
    GROUP BY  
        customer_id
    HAVING
        COUNT(*) > 1  
),

refunded_customers AS (
    SELECT
        period_id,
        period_name,
        item_id,
        COUNT(DISTINCT customer_id) AS refunded_customer_count,
        SUM(payment_amount) AS total_refund_amount
    FROM 
        mart.f_sales
    WHERE 
        status = 'refunded'  -- Условия для возвратов
    GROUP BY  
        customer_id
)

INSERT INTO mart.f_customer_retention (
    period_id,  -- Идентификатор периода (номер недели или номер месяца)
    period_name,  -- Название периода (weekly)
    item_id,  -- Идентификатор категории товара
    new_customers_count,  -- Кол-во новых клиентов
    returning_customers_count,  -- Кол-во вернувшихся клиентов
    refunded_customer_count,  -- Кол-во клиентов, оформивших возврат
    new_customers_revenue,  -- Доход с новых клиентов
    returning_customers_revenue,  -- Доход с вернувшимися клиентами
    customers_refunded  -- Количество возвратов клиентов
)
SELECT 
    substr(cal.week_of_year_iso, 1, 8) AS period_id, 
    substr(cal.week_of_year_iso, 1, 8) AS period_name, 
    nc.item_id AS item_id,  -- Убрано COALESCE
    COUNT(DISTINCT nc.customer_id) AS new_customers_count,  -- Кол-во новых клиентов
    COUNT(DISTINCT rc.customer_id) AS returning_customers_count,  -- Кол-во вернувшихся клиентов
    COUNT(DISTINCT fc.customer_id) AS refunded_customer_count,  -- Кол-во клиентов, оформивших возврат
    SUM(nc.payment_amount) AS new_customers_revenue,  -- Доход с новых клиентов
    SUM(rc.payment_amount) AS returning_customers_revenue,  -- Доход с вернувшимися клиентами
    COUNT(fc.customer_id) AS customers_refunded  -- Количество возвратов клиентов
FROM 
    mart.d_calendar cal
LEFT JOIN 
    new_customers nc ON nc.period_name = substr(cal.week_of_year_iso, 1, 8)
LEFT JOIN 
    returning_customers rc ON rc.period_name = substr(cal.week_of_year_iso, 1, 8) AND nc.item_id = rc.item_id
LEFT JOIN 
    refunded_customers fc ON fc.period_name = substr(cal.week_of_year_iso, 1, 8) AND nc.item_id = fc.item_id
WHERE 
    '{{ ds }}' BETWEEN cal.first_day_of_week AND cal.last_day_of_week
GROUP BY  
    substr(cal.week_of_year_iso, 1, 8), nc.item_id;  -- Группировка по item_id
    
    
    
    
    
    
    
    
    