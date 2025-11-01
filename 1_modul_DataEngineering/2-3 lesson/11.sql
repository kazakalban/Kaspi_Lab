-- Процедуры

-- отличие процедуры от функции
-- * не возвращают значение!!! (ну разве что в out параметрах)
-- * нельзя использовать в select
-- * не кэшируют результат выполнения
-- * могут выполнять commit/rollback


-- простая процедура
DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (i int);

CREATE or replace PROCEDURE insert_data(a integer, b integer)
LANGUAGE SQL
AS $$
INSERT INTO tbl VALUES (a);
INSERT INTO tbl VALUES (b);
$$;

-- вызовем процедуру используя CALL
CALL insert_data(1, 2);

SELECT * FROM tbl;


-- посложнее
DROP TABLE IF EXISTS users;
CREATE TABLE users (name text, email text, department text, created_at timestamp);

CREATE OR REPLACE PROCEDURE add_user(
    user_name TEXT,
    user_email TEXT,
    user_department TEXT DEFAULT 'General'
)
AS $$
BEGIN
    INSERT INTO users (name, email, department, created_at)
    VALUES (user_name, user_email, user_department, CURRENT_TIMESTAMP);
    
    RAISE NOTICE 'Пользователь % добавлен в отдел %', user_name, user_department;
END;
$$ LANGUAGE plpgsql;

-- Вызов процедуры
CALL add_user('Иван Петров', 'ivan@company.com', 'IT');
CALL add_user('Мария Сидорова', 'maria@company.com');

table users;


-- варианты с использованием OUT переменных
-- Процедура, возвращающая несколько значений:
DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items (id serial, order_id int, quantity int, unit_price numeric);
insert into order_items values
        (1,1,10,100),
        (2,123,20,200),
        (3,123,30,50);

CREATE OR REPLACE PROCEDURE calculate_order_stats(
    order_id INTEGER,
    OUT total_amount NUMERIC,
    OUT item_count INTEGER,
    OUT average_price NUMERIC
)
AS $$
BEGIN
    SELECT 
        SUM(oi.quantity * oi.unit_price),
        COUNT(oi.id),
        AVG(oi.unit_price)
    INTO
        total_amount,
        item_count,
        average_price
    FROM order_items oi
    WHERE oi.order_id = calculate_order_stats.order_id;
    
    IF total_amount IS NULL THEN
        RAISE EXCEPTION 'Заказ с ID % не найден', order_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Вызов с получением выходных параметров
-- вывод на экран
CALL calculate_order_stats(123, NULL, NULL, NULL);

-- вопрос как использовать полученные значения? 
-- для этого вызов должен быть из другой процедуры или анонимной процедуры
DO $$
DECLARE
    total NUMERIC;
    count INTEGER;
    avg_price NUMERIC;
    i int;
BEGIN
    i = 1;
    CALL calculate_order_stats(i, total, count, avg_price);
    RAISE NOTICE 'Заказ %: сумма=%, товаров=%, средняя цена=%',i, total, count, avg_price;
END $$;
