create table accounts (
    id int,
    balance numeric(10,2)
);


-- Автоматическое управление транзакциями:
CREATE OR REPLACE PROCEDURE automatic_transaction_example()
AS $$
BEGIN
    -- Начало транзакции (неявно)
    INSERT INTO accounts (id, balance) VALUES (1, 1000);
    UPDATE accounts SET balance = balance - 100 WHERE id = 1;
    
    -- Если все успешно - COMMIT
    -- Если ошибка - ROLLBACK
END;
$$ LANGUAGE plpgsql;

call automatic_transaction_example();
table accounts;


-- Явное управление транзакциями:
CREATE OR REPLACE PROCEDURE explicit_transaction_control()
AS $$
BEGIN
    -- Можно использовать COMMIT и ROLLBACK
    INSERT INTO accounts (id, balance) VALUES (2, 100);
    
    COMMIT;  -- Фиксируем первую часть
    
    INSERT INTO accounts (id, balance) VALUES (1, 3000);
    
    -- В случае ошибки здесь, первая часть уже закоммичена
END;
$$ LANGUAGE plpgsql;
