-- Посмотрим разницу с SQL
CREATE or REPLACE FUNCTION increment(integer) RETURNS integer
    AS $$ SELECT $1 + 1; $$
LANGUAGE SQL;

-- необходимо указывать RETURN
-- появляется BEGIN/END
CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$
BEGIN
    RETURN i + 1;
END;
$$ LANGUAGE plpgsql;

SELECT increment(3);

-- DECLARE
CREATE OR REPLACE FUNCTION increment3(i integer) RETURNS integer AS $$
DECLARE
    inc integer;
BEGIN
    inc = i + 1;
    RETURN inc;
END;
$$ LANGUAGE plpgsql;

SELECT increment3(33);

-- можно преобразовывать переменные
CREATE OR REPLACE FUNCTION increment4(i integer) RETURNS text AS $$
DECLARE
    t text;
BEGIN
    t = (i + 1)::text;
    RETURN t;
END;
$$ LANGUAGE plpgsql;

SELECT increment4(33);

-- PL/pgSQL работает медленнее чем SQL
-- если можете писать на SQL - пишите на SQL или если что-то сложнее, то на PL/pgSQL