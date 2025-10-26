-- классика
CREATE OR REPLACE FUNCTION sales_tax(subtotal real) RETURNS real AS $$
BEGIN
    RETURN subtotal * 0.06;
END;
$$ LANGUAGE plpgsql;

select sales_tax(200);


-- вернуть значение для каждой строчки из таблицы
drop table if exists sales;
create table sales(summa decimal);
insert into sales values (100), (200), (300);

SELECT * FROM sales;
SELECT sales_tax(cast(sales.summa as real)) FROM sales;


-- не вернуть ничего void
-- слово RETURN можно не писать
CREATE OR REPLACE FUNCTION sales_insert(sub real) returns void AS $$
BEGIN
    insert into sales values (sub);
END;
$$ LANGUAGE plpgsql;

select sales_insert(500);
table sales;


-- Если результат выполнения функции не важен, то можно использовать PERFORM
-- но только в вызове из другой процедуры!
CREATE OR REPLACE FUNCTION foo()
RETURNS void AS $$
BEGIN
  RAISE NOTICE 'Hello from void function';
END;
$$ LANGUAGE plpgsql;

-- direct call from SQL
SELECT foo();

-- not allowed
PERFORM foo();


-- in PLpgSQL
DO $$
BEGIN
  SELECT foo(); -- is not allowed
  PERFORM foo(); -- is ok
END;
$$;

-- или функции
CREATE OR REPLACE FUNCTION foo2()
RETURNS void AS $$
BEGIN
  PERFORM foo();
END;
$$ LANGUAGE plpgsql;

select foo2();

-- Например можем заполнить другую таблицу результами на основе текущей
drop table if exists sales2;
create table sales2(kolvo decimal, price decimal);
insert into sales2 values (100,10), (200,20), (300,30);


-- интересная штука - таблицы нет
CREATE OR REPLACE FUNCTION sales_total(kolvo decimal, price decimal) returns void AS $$
BEGIN
    insert into sales_total values (kolvo*price);
END;
$$ LANGUAGE plpgsql;

drop table if exists sales_total;
create table sales_total(summa decimal);

SELECT sales_total(kolvo, price) FROM sales2;

-- альтернатива select * from
table sales_total;
