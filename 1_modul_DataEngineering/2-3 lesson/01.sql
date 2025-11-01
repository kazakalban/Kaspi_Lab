SELECT current_database();

drop function if exists add;
CREATE FUNCTION add(integer, integer) RETURNS integer
    AS 'SELECT $1 + $2;'
LANGUAGE SQL;

-- 2 варианта обращения к функции
SELECT add(3,4);
SELECT * from add(3,4);


CREATE or replace FUNCTION add2(integer, integer default 42) RETURNS integer
    AS 'SELECT $1 + $2;'
LANGUAGE SQL;

SELECT add2(20, null);
SELECT add2(20);


-- экранируем с использованием $$
CREATE or REPLACE FUNCTION increment(integer) RETURNS integer
    AS $$ SELECT $1 + 1; $$
LANGUAGE SQL;

SELECT increment(3);


-- экранируем с использованием $имя_функции$
CREATE OR REPLACE FUNCTION increment2(i integer) RETURNS integer AS 
$increment2$ SELECT $1 + 1; $increment2$ 
LANGUAGE sql;

SELECT increment2(33);

-- где конкретно указывать параметры функции не важно, но обычно в конце
CREATE OR REPLACE FUNCTION increment3(i integer) RETURNS integer 
LANGUAGE sql AS $increment3$
    SELECT i + 1;
$increment3$;

SELECT increment3(299);
