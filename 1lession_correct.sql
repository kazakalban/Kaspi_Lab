--select * from customer_orders order by order_id;
--select * from customer_orders_copy order by order_id;
--drop table if exists customer_orders_copy;  

-- создать копию
create table customer_orders_copy as 
select * 
from customer_orders;

-- очистка колонки cutomer_name
update customer_orders_copy
set customer_name = initcap(lower(trim(customer_name)));

update customer_orders_copy
set customer_name = null
where customer_name ilike 'null';

-- очистка колонки email
update customer_orders_copy
set email = null
where email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';

-- очистка колонки order_date
update  customer_orders_copy
set order_date = to_date(order_date, 'MM/DD/YYYY')
where order_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$';

update  customer_orders_copy
set order_date = to_date(order_date, 'YYYY/MM/DD')
where order_date ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$';

update  customer_orders_copy
set order_date = to_date(order_date, 'YYYY-MM-DD')
where order_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

alter table customer_orders_copy 
alter column order_date type date
using to_date(order_date, 'YYYY-MM-DD');

-- очистка колонки product_name
update customer_orders_copy
set product_name = lower(trim(product_name));

update customer_orders_copy
set product_name = null
where product_name ilike 'null';

-- очистка колонки quantity
update customer_orders_copy
set quantity = TRIM(quantity);

update customer_orders_copy
set quantity = null
where quantity !~ '^[0-9]+$';

alter table customer_orders_copy 
alter column quantity type integer
using quantity::integer;

-- очистка колонки price
update customer_orders_copy
set price = trim(price);

update customer_orders_copy
set price = regexp_replace(price, '[^0-9.,]', '','g');

update customer_orders_copy
set price = replace(price, ',', '.');

update customer_orders_copy
set price = null 
where price !~ '^[0-9]+(\.[0-9]+)?$';

alter table customer_orders_copy 
alter column price type numeric 
using price::numeric;

-- очистка колонки country
update customer_orders_copy
set country = initcap(LOWER(TRIM(country)));

update customer_orders_copy
set country = 
case
	when country in ('Usa','Us') then 'United States'
	when country in ('Uk') then 'United Kingdom'
	else country
end;

-- очистка колонки order_status
update customer_orders_copy
set order_status = lower(trim(order_status));

-- удаляем колонку notes
alter table customer_orders_copy 
drop column notes;

--убираем дубликаты
with row_cte as (
select 
*,
row_number() over (partition by customer_name, email, order_date, product_name, quantity, price, country, order_status) as row_num
from customer_orders_copy
)
delete from customer_orders_copy as cop
using row_cte as rw
where cop.order_id = rw.order_id and rw.row_num > 1;

















