-- Практика 3

-- 1. Создать функцию, которая:
-- производит чистку данных в таблице customer_orders
-- чистые данные помещает в таблицу cleaned_customer_orders
-- данные, которые невозможно пофиксить, поместить в таблицу problem_customer_orders с описанием причины
-- возвращает количество проблемных записей

CREATE OR REPLACE FUNCTION clenar_table(table_name text)
RETURNS void AS
$$
DECLARE
	new_table text := table_name || '_copy'::text;
    sql_query text;
	problem_count integer;
BEGIN
	EXECUTE format('create table %I as select * from %I', new_table, table_name);
    /*
     * Формируем SQL-запрос динамически через format().
     * %I — безопасная подстановка имени таблицы (идентификатора).
     */
    sql_query := format($fmt$
        UPDATE %I
        SET
            -- Чистим имя клиента
            customer_name = CASE
                WHEN customer_name IS NULL OR trim(customer_name) = '' THEN NULL
                ELSE initcap(lower(trim(customer_name))) -- убираем пробелы и делаем "John Smith"
            END,

            -- Приводим email к нижнему регистру, если он валидный
            email = CASE
                WHEN email IS NULL OR trim(email) = '' THEN NULL
                WHEN email !~ '^[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN NULL
                ELSE lower(trim(email))
            END,

            -- Приводим страну к единому виду
            country = CASE
                WHEN country IS NULL THEN NULL
                WHEN country ILIKE 'null' THEN NULL
                WHEN country IN ('Usa', 'USA', 'Us') THEN 'United States'
                WHEN country IN ('UK', 'Uk') THEN 'United Kingdom'
                ELSE initcap(lower(trim(country))) -- всё остальное аккуратно форматируем
            END,

            -- Преобразуем числовые строки в нормальный numeric
            -- Работает для "1,234.56", "1234,56", "1234"
            price = CASE
    			WHEN price IS NULL OR trim(price) = '' THEN NULL

    			-- Если после удаления всех символов, кроме цифр, запятых и точек — пусто, значит там мусор
    			WHEN regexp_replace(trim(price), '[^0-9.,]', '', 'g') = '' THEN NULL

    			ELSE (
        			regexp_replace(
            		-- Убираем всё, кроме цифр, запятых и точек (например $, ₸, €, пробелы)
            		regexp_replace(trim(price), '[^0-9.,]', '', 'g'),
            		-- Убираем запятые, если они используются как разделитель тысяч
            		',(?=[0-9]{3}(\.|$))', '', 'g'
        		))
			END

        $fmt$, new_table);

    -- Выполняем динамический SQL
    EXECUTE sql_query;

    -- Удаление дубликатов
	EXECUTE format ('SELECT DISTINCT ON (customer_name, email, order_date, product_name) * FROM %I ORDER BY 
								customer_name, 
								email, 
								order_date, 
								product_name, order_id', new_table);
    -- чистые данные помещает в таблицу cleaned_customer_orders_copy
    EXECUTE format('CREATE TABLE IF NOT EXISTS cleaned_%I AS SELECT * FROM %I', new_table, new_table);

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
    EXECUTE format(
        'INSERT INTO problem_%I
        SELECT * FROM %I
        WHERE
            customer_name IS NULL OR
            email IS NULL OR
            order_date IS NULL OR
            product_name IS NULL OR
            quantity IS NULL OR
            price IS NULL OR
            country IS NULL OR
        order_status IS NULL', new_table, new_table);

    EXECUTE format('SELECT COUNT(*) FROM problem_%I', new_table) INTO problem_count;
    RAISE NOTICE 'База успешно почистилась. количество проблемных записей: %', problem_count;
END;
$$ LANGUAGE plpgsql;

SELECT clenar_table('customer_orders');
