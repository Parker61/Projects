-- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные,
-- которые будут добавлены или изменены после этой даты
insert_load_date AS ( 
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customer_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
        FROM dwh_delta
)
