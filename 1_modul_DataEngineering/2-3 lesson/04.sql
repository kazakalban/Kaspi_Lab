-- примеры объявления параметров
-- безымянные - по номеру входящей переменной
-- int == integer
CREATE OR REPLACE FUNCTION instr(int, integer) RETURNS integer AS $$
BEGIN
    return $1 + $2;
END;
$$ LANGUAGE plpgsql;

select instr('1',2);


-- самый корректный вариант - сразу задать имена переменным
CREATE OR REPLACE FUNCTION instr3(i int, y int) RETURNS integer AS $$
BEGIN
    return i + y;
END;
$$ LANGUAGE plpgsql;

select instr3(1,2);

-- или используя OUT переменные
CREATE OR REPLACE FUNCTION instr4(i int, y int, out sum int, out prod int) AS $$
BEGIN
    sum = i + y;
	prod = i*y;
END;
$$ LANGUAGE plpgsql;

select instr4(1,2);
select * from  instr4(1,2);

-- использование дефолтного значения при передаче параметров
CREATE OR REPLACE FUNCTION instr5(i int, y int default 100) returns int AS $$
BEGIN
    return i + y;
END;
$$ LANGUAGE plpgsql;

select instr5(1);


-- DECLARE
-- обратите внимание - создали локальную переменную с таким же именем i
CREATE OR REPLACE FUNCTION decl(i integer) RETURNS text AS $$
DECLARE
    i integer default 100;
    d timestamp DEFAULT now();
BEGIN
    return i || ', ' || decl.i;
END;
$$ LANGUAGE plpgsql;

select decl(1);