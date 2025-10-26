-- использование составного типа
CREATE TYPE currency AS (
    amount numeric,
    code   text
);
drop table if exists transactions;
CREATE TABLE transactions(
    account_id   integer,
    debit        currency,
    credit       currency,
    date_entered date DEFAULT current_date
);

INSERT INTO transactions VALUES(2, (350.00,'KZT'), NULL);
INSERT INTO transactions VALUES(1, NULL, (7000.00,'KZT'));
INSERT INTO transactions VALUES(3, (20.00,'KZT'), NULL);

SELECT * FROM transactions;

-- Дальше мы можем создать функции для работы с этим типом. Например:
CREATE FUNCTION multiply(factor numeric, cur currency) RETURNS currency AS $$
    SELECT ROW(factor * cur.amount, cur.code)::currency;
$$ IMMUTABLE LANGUAGE SQL;

SELECT
    account_id,
    multiply(2,debit),
    multiply(2,credit),
    date_entered
FROM transactions;


-- но если мы попробуем использовать стандартные методы, то они не сработают
SELECT account_id, 1.2 * debit, 2 * credit, date_entered FROM transactions;

-- Мы можем даже определить оператор умножения:
CREATE OPERATOR * (
    PROCEDURE = multiply,
    LEFTARG = numeric,
    RIGHTARG = currency
);

-- И использовать его в выражениях:
SELECT account_id, 1.2 * debit, 2 * credit, date_entered FROM transactions;