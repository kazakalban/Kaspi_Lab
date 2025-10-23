CREATE TABLE public.customer_orders_update AS
    select 
    	order_id::int AS order_id,
        customer_name,
        email, 
        order_date::date AS  order_date, 
        product_name, 
        quantity, 
        price, 
        country, 
        order_status, 
        notes
    from public.customer_orders;

# коректировать поле customer_name, product_name, order_status, country
update public.customer_orders_update
    set customer_name = initcap(customer_name)
update public.customer_orders_update
	set product_name = initcap(product_name)
update public.customer_orders_update
	set order_status = lower(order_status)
update public.customer_orders_update
	set country = initcap(country)
	
# поле quantity
update public.customer_orders_update
	set quantity = 2
    where quantity = 'two'

# поле price убрать вальюту 
update public.customer_orders_update
	set price = REPLACE(price, '$', '')

# поле price убрать запятая
update public.customer_orders_update
    set price = REPLACE(price, ',', '');

# поле country 
update public.customer_orders_update
	set country = 
	case
		when country in('Usa','Us') then 'United Station'
		when country in('Uk') then 'United Kingdom'
		else country
	end;

# и удаление notes
alter table public.customer_orders_update
drop column notes;

# удаление дубликатов
select distinct on (customer_name, email, order_date, product_name, quantity, price)
from public.customer_orders_update order by (customer_name, email, order_date, product_name, quantity, price, order_id)
