CREATE TABLE public.customer_orders_update AS TABLE public.customer_orders; # Создание резерв копии ориг таблицы

CREATE TABLE public.customer_orders_update AS
    SELECT
        order_id::int AS order_id,
        order_date::date AS  order_date,
        price::numeric AS price

    FROM public.customer_orders;
        


SELECT order_id, customer_name, email, order_date, product_name, quantity, price, country, order_status, notes
FROM public.customer_orders_update;  # Выбор всех данных из обн таблицы

# order_id
UPDATE public.customer_orders_update 
    SET order_id = REPLACE(order_id, ',', ''); # order_id удалил запятые

# customer_name, product_name+, order_status+, country+
UPDATE public.customer_orders_update 
    SET customer_name = INITCAP(customer_name), 
    product_name = INITCAP(product_name),
    order_status = INITCAP(order_status), 
    country = INITCAP(country); # customer_name и ... чтобы каждое слово начиналось с заглавной буквы

# email
UPDATE public.customer_orders_update 
    SET email = LOWER(email); #  email все буквы строчными

# order_date
UPDATE public.customer_orders_update 
    SET order_date = REPLACE(order_date, '/', '-');
UPDATE public.customer_orders_update 
    SET order_date = CAST(order_date as DATETIME); # order_date строки в формат даты.  ERROR

# quantity +
UPDATE public.customer_orders_update 
    SET quantity = 2
    WHERE quantity = 'two'; # quantity строки в целые числа

# price
UPDATE public.customer_orders_update 
    SET price = REPLACE(price, ',', '');
UPDATE public.customer_orders_update 
    SET price = REPLACE(price, '$', '');

#country +
UPDATE public.customer_orders_update
    SET country = 'United States' 
    WHERE country = 'Usa';

UPDATE public.customer_orders_update
    SET country = 'United States' 
    WHERE country = 'Us';

UPDATE public.customer_orders_update
    SET country = 'United Kingdom'
    WHERE country = 'Uk'; # country 'Uk' на 'United Kingdom'

# notes +
UPDATE public.customer_orders_update 
    SET notes = TRIM(notes); # notes удалил пробелы в начале и конце
