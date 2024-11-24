DROP TABLE IF EXISTS mart.f_customer_retention;

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    id serial4 NOT NULL,
    new_customers_count int4 NOT NULL,
    returning_customers_count int4 NOT NULL,
    refunded_customer_count int4 NOT NULL,
    period_name varchar(20) NOT NULL, -- Изменено на varchar
    period_id int4 NULL,
    item_id int4 NULL,
    new_customers_revenue numeric(12,2) NULL, -- Изменено на numeric
    returning_customers_revenue numeric(12,2) NULL, -- Изменено на numeric
    customers_refunded int4 NULL,
    
    CONSTRAINT f_customer_retention_item_id_fkey FOREIGN KEY (item_id)
        REFERENCES mart.d_item (item_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION);

	CREATE INDEX IF NOT EXISTS f_cr2
    ON mart.f_customer_retention USING btree
    (item_id ASC NULLS LAST)
    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS f_cr3
    ON mart.f_customer_retention USING btree
    (period_id ASC NULLS LAST)
    TABLESPACE pg_default;
	
    CREATE INDEX IF NOT EXISTS f_cr4
    ON mart.f_customer_retention USING btree
    (period_name ASC NULLS LAST)
    TABLESPACE pg_default;


comment on column mart.f_customer_retention.new_customers_count is 'кол-во новых клиентов (тех, которые сделали только один заказ за рассматриваемый промежуток времени)';
comment on column mart.f_customer_retention.returning_customers_count is 'кол-во вернувшихся клиентов (тех, которые сделали только несколько заказов за рассматриваемый промежуток времени).' ;
comment on column mart.f_customer_retention.refunded_customer_count is 'кол-во клиентов, оформивших возврат за рассматриваемый промежуток времени.' ;
comment on column mart.f_customer_retention.period_name is 'weekly' ;
comment on column mart.f_customer_retention.period_id is 'идентификатор периода (номер недели или номер месяца).' ;
comment on column mart.f_customer_retention.item_id is 'идентификатор категории товара.' ;
comment on column mart.f_customer_retention.new_customers_revenue is 'доход с новых клиентов' ;
comment on column mart.f_customer_retention.returning_customers_revenue is 'доход с вернувшихся клиентов';
comment on column mart.f_customer_retention.customers_refunded is 'количество возвратов клиентов'