-- Практика 3

-- 1. Создать функцию, которая:
-- производит чистку данных в таблице customer_orders
-- чистые данные помещает в таблицу cleaned_customer_orders
-- данные, которые невозможно пофиксить, поместить в таблицу problem_customer_orders с описанием причины
-- возвращает количество проблемных записей

CREATE OR REPLACE FUNCTION clenar_table(table_name text) RETURNS void AS $$
DECLARE 
	new_table text := table_name || '_copy';
BEGIN
	--EXECUTE выполняет динамический sql
	--execute format('create table %I as select * from %I', new_table, table_name);
	EXECUTE format($fmt$
    UPDATE %I
    SET
        customer_name = CASE
            WHEN customer_name ILIKE 'null' THEN NULL
            ELSE initcap(lower(trim(customer_name)))
        END,
        email = CASE
            WHEN email !~ '^[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN NULL
            ELSE lower(trim(email))
        END
$fmt$, new_table);

			
END;
$$ LANGUAGE plpgsql;

SELECT clenar_table('customer_orders');