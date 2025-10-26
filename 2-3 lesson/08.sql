-- вернем таблицу
drop TABLE if exists sales;
CREATE TABLE sales (itemno int, price int, quantity int);
INSERT INTO sales VALUES (1,10,100),(2,20,200);

-- простейший вариант TABLE
drop function if exists extended_sales;
CREATE or replace FUNCTION extended_sales(p_itemno int)
RETURNS TABLE(quantity int, total int) AS $$
BEGIN
    RETURN QUERY SELECT s.quantity as a, s.quantity * s.price as b FROM sales s
                 WHERE s.itemno = p_itemno;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM sales;
SELECT extended_sales(1);
SELECT * from extended_sales(1);

SELECT itemno, extended_sales(itemno) from sales;


-- Пример использования RETURN NEXT:
drop table if exists foo;
CREATE TABLE foo (fooid INT, foosubid INT, fooname TEXT);

INSERT INTO foo VALUES (1, 2, 'three');
INSERT INTO foo VALUES (4, 5, 'six');

-- указываем тип данных по имени существующей таблицы
CREATE OR REPLACE FUNCTION get_all_foo() RETURNS SETOF foo AS
$BODY$
DECLARE
    r foo%rowtype; -- тип данных кортеж
    --r record;
BEGIN
    FOR r IN
        SELECT * FROM foo WHERE fooid > 0
    LOOP
        -- здесь возможна обработка данных
        r.fooid := r.fooid + 10;
        RETURN NEXT r; -- возвращается текущая строка запроса
    END LOOP;
    RETURN;
END
$BODY$
LANGUAGE plpgsql;

SELECT * FROM get_all_foo();
SELECT * FROM foo;


-- более сложный пример возврата таблицы (join)
drop table if exists users;
create table if not exists users (
    id int, 
    name text, 
    email text,
    created_at date default current_timestamp, 
    status text
);
insert into users values 
    (1,'Ivan','i@i.i', '20250922', 'waiting'),
    (2,'Petr','p@p.p', '20250922','active');
table users;

drop table if exists orders;
create table if not exists orders (
    id int, 
    user_id int, 
    amount numeric,
    created_at date default current_timestamp, 
    status text
);
insert into orders values 
    (1,1,15000,'20250922','delivered'),
    (2,2,2000, '20250922','active');
table orders;


CREATE OR REPLACE FUNCTION get_orders_in_date_range(
    start_date DATE, 
    end_date DATE,
    min_amount NUMERIC DEFAULT 0
)
RETURNS TABLE(
    order_id INTEGER,
    customer_name TEXT,
    order_amount NUMERIC,
    order_date DATE,
    status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        o.id,
        u.name,
        o.amount,
        o.created_at::DATE,
        o.status
    FROM orders o
    JOIN users u ON o.user_id = u.id
    WHERE o.created_at BETWEEN start_date AND end_date
      AND o.amount >= min_amount
    ORDER BY o.created_at DESC;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_orders_in_date_range('2025-09-01', '2025-09-30', 1000);


-- + CASE
CREATE OR REPLACE FUNCTION search_users(
    search_term TEXT DEFAULT NULL,
    user_status TEXT DEFAULT NULL
)
RETURNS TABLE(
    id INTEGER,
    name TEXT,
    email TEXT,
    status TEXT,
    match_type TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        u.id,
        u.name,
        u.email,
        u.status,
        CASE 
            WHEN search_term IS NOT NULL AND u.name ILIKE '%' || search_term || '%' THEN 'name_match'
            WHEN search_term IS NOT NULL AND u.email ILIKE '%' || search_term || '%' THEN 'email_match'
            ELSE 'no_match'
        END as match_type
    FROM users u
    WHERE 
        (search_term IS NULL OR 
         u.name ILIKE '%' || search_term || '%' OR 
         u.email ILIKE '%' || search_term || '%')
        AND (user_status IS NULL OR u.status = user_status)
    ORDER BY
        CASE WHEN search_term IS NOT NULL THEN
            CASE 
                WHEN u.name ILIKE '%' || search_term || '%' THEN 1
                WHEN u.email ILIKE '%' || search_term || '%' THEN 2
                ELSE 3
            END
        ELSE 4
        END;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM search_users('Petr', 'active');
SELECT * FROM search_users(user_status => 'active');