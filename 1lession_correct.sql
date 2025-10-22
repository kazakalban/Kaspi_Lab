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

# 
update public.customer_orders_update
    set customer_name = initcap(customer_name)

update public.customer_orders_update
	set product_name = initcap(product_name)

update public.customer_orders_update
	set order_status = lower(order_status)

update public.customer_orders_update
	set country = initcap(country)
	
#
update public.customer_orders_update
	set quantity = 2
    where quantity = 'two'

#
update public.customer_orders_update
	set price = REPLACE(price, '$', '')

#
update public.customer_orders_update
    set price = REPLACE(price, ',', '');

#
update public.customer_orders_update
	set country = 'United States' 
    where country = 'Usa';

update public.customer_orders_update
    set country = 'United States' 
    where country = 'Us';

update public.customer_orders_update
    set country = 'United Kingdom'
    where country = 'Uk'; 