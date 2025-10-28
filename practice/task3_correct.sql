-- Практика 3

-- 1. Создать функцию, которая:
-- производит чистку данных в таблице customer_orders
-- чистые данные помещает в таблицу cleaned_customer_orders
-- данные, которые невозможно пофиксить, поместить в таблицу problem_customer_orders с описанием причины
-- возвращает количество проблемных записей

-- производит чистку данных в таблице customer_orders

CREATE OR REPLACE FUNCTION clean_table(table_name text)
RETURNS void AS
$$
DECLARE
    new_table text := table_name || '_copy';
    sql_query text;
BEGIN
    -- Создаем копию таблицы
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I AS SELECT * FROM %I', new_table, table_name);

    -- Чистим данные
    sql_query := format($fmt$
        UPDATE %I
        SET
            customer_name = CASE
				WHEN customer_name IS NULL OR lower(trim(customer_name)) = 'null' THEN NULL
                WHEN customer_name IS NULL OR trim(customer_name) = '' THEN NULL
                ELSE initcap(lower(trim(customer_name)))
            END,

            email = CASE
                WHEN email IS NULL OR trim(email) = '' THEN NULL
                WHEN email !~ '^[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN NULL
                ELSE lower(trim(email))
            END,
            -- Очистка order_date
            order_date = CASE
                WHEN order_date::text ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' 
                    THEN to_date(order_date, 'MM/DD/YYYY')
                WHEN order_date::text ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
                    THEN to_date(order_date, 'YYYY/MM/DD')
                WHEN order_date::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                    THEN to_date(order_date, 'YYYY-MM-DD')
                ELSE NULL
            END,
            -- Очистка product_name
            product_name = CASE
                WHEN product_name ILIKE 'null' THEN NULL
                ELSE lower(trim(product_name))
            END,

            -- Очистка quantity
            quantity = CASE
                WHEN quantity !~ '^[0-9]+$' THEN NULL
                ELSE TRIM(quantity)::integer
            END,

            country = CASE
                WHEN country IS NULL THEN NULL
                WHEN country ILIKE 'null' THEN NULL
                WHEN lower(trim(country)) IN ('usa', 'us') THEN 'United States'
                WHEN lower(trim(country)) IN ('uk') THEN 'United Kingdom'
                ELSE initcap(lower(trim(country)))
            END,

            price = CASE
                WHEN price IS NULL OR trim(price) = '' THEN NULL
                WHEN regexp_replace(trim(price), '[^0-9.,]', '', 'g') = '' THEN NULL
                ELSE (
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(trim(price), '[^0-9.,]', '', 'g'),
                            ',(?=[0-9]{3}(\.|$))', '', 'g'
                        ),
                        ',', '.', 'g'
                    )::numeric
                )
            END,
            -- Очистка order_status
            order_status = CASE
                WHEN order_status IS NULL THEN NULL
                ELSE lower(trim(order_status))
            END
    $fmt$, new_table);

    EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION delete_duplicates(table_name text)
RETURNS void AS
$$
DECLARE
    sql_query text;
BEGIN
    sql_query := format($fmt$
        DELETE FROM %I a
        USING %I b
        WHERE a.ctid < b.ctid
          AND a.customer_name = b.customer_name
          AND a.email = b.email
          AND a.order_date = b.order_date
          AND a.product_name = b.product_name
    $fmt$, table_name, table_name);

    EXECUTE sql_query;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_clean_table(table_name text)
RETURNS void AS
$$
DECLARE
    cleaned_table text := 'cleaned_' || table_name;
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I AS SELECT * FROM %I WHERE FALSE', cleaned_table, table_name);

    EXECUTE format($fmt$
        INSERT INTO %I
        SELECT * FROM %I
        WHERE
            customer_name IS NOT NULL AND
            email IS NOT NULL AND
            order_date IS NOT NULL AND
            product_name IS NOT NULL AND
            quantity IS NOT NULL AND
            price IS NOT NULL AND
            country IS NOT NULL AND
            order_status IS NOT NULL
    $fmt$, cleaned_table, table_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION copy_problems(table_name text)
RETURNS void AS
$$
DECLARE
    problem_table text := 'problem_' || table_name;
    problem_count int;
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I AS SELECT * FROM %I WHERE FALSE', problem_table, table_name);

    EXECUTE format(
		'
        INSERT INTO %I
        SELECT * FROM %I
        WHERE
            customer_name IS NULL OR
            email IS NULL OR
            order_date IS NULL OR
            product_name IS NULL OR
            quantity IS NULL OR
            price IS NULL OR
            country IS NULL OR
            order_status IS NULL',
    problem_table, table_name);
    -- Считаем количество строк в problem_table
    EXECUTE format('SELECT COUNT(*) FROM %I', problem_table)
	INTO problem_count;

    -- Выводим сообщение в консоль
    RAISE NOTICE 'Количество проблемных строк: %', problem_count;

END;
$$ LANGUAGE plpgsql;



SELECT clean_table('customer_orders')
SELECT delete_duplicates('customer_orders_copy')
SELECT create_clean_table('customer_orders_copy')
SELECT copy_problems('customer_orders_copy')

