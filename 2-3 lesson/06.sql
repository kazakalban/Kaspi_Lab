-- record

drop table if exists employees cascade;
create table if not exists employees (
    id serial, 
    name text, 
    salary numeric
);
insert into employees(name, salary) values ('Ivanov', 100500),('Petrov',300600),('SIdorov',200000);

CREATE OR REPLACE FUNCTION t1 () returns void as $$
DECLARE
    row_record RECORD;
BEGIN
    FOR row_record IN 
        SELECT id, name, salary FROM employees 
    LOOP
        RAISE NOTICE 'Employee %: % (Salary: %)', 
            row_record.id, 
            row_record.name, 
            row_record.salary;
    END LOOP;
END;
$$ language plpgsql;

select t1();


-- Сохранение результата одного столбца или выражения
CREATE OR REPLACE FUNCTION t2 (i int) returns void as $$
DECLARE
    temp_record RECORD;
BEGIN
    SELECT id, name INTO temp_record FROM employees WHERE id = i;
    RAISE NOTICE 'Employee: %', temp_record.name;
END;
$$ language plpgsql;

select t2(1);


-- Динамические запросы с неизвестной структурой
CREATE OR REPLACE FUNCTION t3 (table_name text) returns void as $$
DECLARE
    target_record RECORD;
BEGIN
    EXECUTE 'SELECT * FROM ' || table_name INTO target_record;
    RAISE NOTICE 'Employee: %', target_record.name;
END;
$$ language plpgsql;

select t3('employees');


-- Возврат наборов строк из функций
CREATE OR REPLACE FUNCTION get_employees2() RETURNS SETOF RECORD AS $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM employees LOOP
        RETURN NEXT r;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;

select get_employees2();
select * from get_employees2();
select * from get_employees2() as employees(id int, name text, salary numeric);


-- итоги

-- select x into base_x (базовый тип например int)
-- select x, y into base_x, base_y
-- select x, y into record (заранее не знаем типы)
-- select case ... into ...
-- execute '...' into ...

-- returns setof base_type типа int
-- returns setof record
-- returns setof table (вернем все колонки - postgres неявно создает тип для строки таблицы)
-- returns setof table (список колонок)

-- по поводу цикла
-- под капотом это курсор
-- можно явно написать в виде курсора
-- но обычно работа с курсором не принята (тяжеловесный)
-- можно использовать в редких случаях (кастомный переход, большие данные, динамичные запросы)
-- правда динамичные можно сделать как for in execute

CREATE OR REPLACE FUNCTION t1_cursor () returns void as $$
DECLARE
    my_cursor CURSOR FOR SELECT id, name, salary FROM employees;
    row_record RECORD;
BEGIN
    OPEN my_cursor;
    LOOP
        FETCH my_cursor INTO row_record;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE 'Employee %: % (Salary: %)',
            row_record.id,
            row_record.name,
            row_record.salary;
    END LOOP;
    CLOSE my_cursor;
END;
$$ language plpgsql;

select t1_cursor();