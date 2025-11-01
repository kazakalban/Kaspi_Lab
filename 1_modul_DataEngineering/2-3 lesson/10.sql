-- кортежи %ROWTYPE
-- Создадим тестовую таблицу
drop table employees;
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    salary NUMERIC(10,2),
    department TEXT,
    hire_date DATE DEFAULT CURRENT_DATE
);
insert into employees(name,salary,department) values ('Ivan',100,'IT');


-- Функция с использованием ROWTYPE
CREATE OR REPLACE FUNCTION get_employee_info(emp_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    emp_record employees%ROWTYPE;  -- Объявление переменной типа строки таблицы
BEGIN
    -- Получаем всю строку в переменную
    SELECT * INTO emp_record 
    FROM employees 
    WHERE id = emp_id;
    
    IF NOT FOUND THEN
        RETURN 'Сотрудник не найден';
    END IF;
    
    -- Доступ к полям через точку
    RETURN format('Сотрудник: %s, Зарплата: %s, Отдел: %s', 
                 emp_record.name, 
                 emp_record.salary, 
                 emp_record.department);
END;
$$ LANGUAGE plpgsql;

select get_employee_info(1);


-- Использование в циклах
CREATE OR REPLACE FUNCTION increase_salary_all(department_filter TEXT, percent NUMERIC)
RETURNS TABLE(emp_name TEXT, old_salary NUMERIC, new_salary NUMERIC) AS $$
DECLARE
    emp employees%ROWTYPE;
BEGIN
    FOR emp IN 
        SELECT * FROM employees 
        WHERE department = department_filter 
        OR department_filter IS NULL
    LOOP
        old_salary := emp.salary;
        emp.salary := emp.salary * (1 + percent/100);
        
        -- Обновляем запись в базе
        UPDATE employees 
        SET salary = emp.salary 
        WHERE id = emp.id;
        
        -- Возвращаем результат
        emp_name := emp.name;
        new_salary := emp.salary;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Работа с триггерами
-- Таблица для аудита
CREATE TABLE employees_audit (
    id SERIAL PRIMARY KEY,
    operation CHAR(1),  -- I, U, D
    employee_id INTEGER,
    old_data text,  -- Вся старая строка
    new_data text,  -- Вся новая строка
    changed_by TEXT DEFAULT CURRENT_USER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Триггерная функция
CREATE OR REPLACE FUNCTION log_employee_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO employees_audit (operation, employee_id, new_data)
        VALUES ('I', NEW.id, NEW);
        
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO employees_audit (operation, employee_id, old_data, new_data)
        VALUES ('U', NEW.id, OLD, NEW);
        
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO employees_audit (operation, employee_id, old_data)
        VALUES ('D', OLD.id, OLD);
    END IF;
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Создание триггера
CREATE TRIGGER employees_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON employees
    FOR EACH ROW EXECUTE FUNCTION log_employee_changes();

-- также можно добавить условие по каким колонкам через OF
-- можно указать дополнительные условия через WHEN


table employees;

insert into employees(name,salary,department) values ('Ivan',100,'IT');

table employees_audit;


