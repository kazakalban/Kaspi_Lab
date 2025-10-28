-- Практика 3

-- 1. Создать функцию, которая:
-- производит чистку данных в таблице customer_orders
-- чистые данные помещает в таблицу cleaned_customer_orders
-- данные, которые невозможно пофиксить, поместить в таблицу problem_customer_orders с описанием причины
-- возвращает количество проблемных записей

CREATE OR REPLACE FUNCTION clenar_table(table_name text) RETURNS void AS $$
DECLARE

    -- new_table будет иметь имя table_name с суффиксом '_copy'
	new_table text := table_name || '_copy';
    -- proplem_count количество проблемных записей
    proplem_count integer;
BEGIN
	--EXECUTE выполняет динамический sql
	--execute format('create table %I as select * from %I', new_table, table_name);
	
    -- update set обновляем данные в таблице
    EXECUTE format($fmt$
    UPDATE %I
    SET
        -- Очистка customer_name
        customer_name = CASE
            WHEN customer_name ILIKE 'null' THEN NULL
            ELSE initcap(lower(trim(customer_name)))
        END,

        -- Очистка email
        email = CASE
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

        -- Очистка price
        price = CASE
            WHEN price IS NULL THEN NULL
            -- регулярная выражения удаляет всё, кроме цифр, точки и запятой
            WHEN regexp_replace(trim(price), '[^0-9.,]', '','g') = '' THEN NULL
            -- заменяет запятую на точку
            ELSE regexp_replace(
                    regexp_replace(trim(price), '[^0-9.,]', '','g'),
                    ',','.','g')::numeric
        END,

        -- Очистка country
        country = CASE
            WHEN country IS NULL THEN NULL
            WHEN country in ('Usa', 'USA', 'Us') THEN 'United States'
            WHEN country in ('UK', 'Uk') THEN 'United Kingdom'
            ELSE lower(trim(country))
        END,

        -- Очистка order_status
        order_status = CASE
            WHEN order_status IS NULL THEN NULL
            ELSE lower(trim(order_status))
        END
    $fmt$, new_table);

    -- Удаление дубликатов

    -- чистые данные помещает в таблицу cleaned_customer_orders_copy
    EXECUTE format('CREATE TABLE IF NOT EXISTS cleaned_%I AS 
                    SELECT * FROM %I WHERE FALSE', new_table, new_table);
    
    EXECUTE format('INSERT INTO cleaned_%I SELECT * FROM %I WHERE 
                    customer_name IS NOT NULL AND 
                    email IS NOT NULL AND 
                    order_date IS NOT NULL AND 
                    product_name IS NOT NULL AND 
                    quantity IS NOT NULL AND 
                    price IS NOT NULL AND 
                    country IS NOT NULL AND 
                    order_status IS NOT NULL', new_table, new_table);



    -- Создание таблицы для проблемных записей
    EXECUTE format('CREATE TABLE IF NOT EXISTS problem_%I AS 
                    SELECT * FROM %I WHERE FALSE', new_table, new_table);
    
    -- Перенос проблемных записей в таблицу problem_customer_orders_copy
    EXECUTE format($fmt$
    INSERT INTO problem_%I
    SELECT * FROM %I
    WHERE
        customer_name IS NULL OR
        email IS NULL OR
        order_date IS NULL OR
        product_name IS NULL OR
        quantity IS NULL OR
        price IS NULL OR
        country IS NULL OR
        order_status IS NULL
    $fmt$, new_table, new_table);

    execute format('SELECT COUNT(*) FROM problem_%I', new_table) INTO proplem_count;
    RAISE NOTICE 'База успешно почитился. количество проблемных записей: %', proplem_count;
END;
$$ LANGUAGE plpgsql;

SELECT clenar_table('customer_orders');
