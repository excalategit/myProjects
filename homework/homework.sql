-- This exercise relates to data insertion and therefore is not appropriate for an OLAP database as is used here.
-- However it serves as a good demonstration of proficiency in SQL Stored Procedures.

CREATE OR REPLACE PROCEDURE sales_update(p_sale_id int, p_user_id int, p_prod_id int, p_qty int)
LANGUAGE plpgsql
AS $$
DECLARE
    v_product_status text;
    v_product_key int;
    v_user_key int;
    v_date_key int;
    v_stock int;
    v_price float;
    
begin  
    select product_key into v_product_key
    from homework.dim_product
    where product_id = p_prod_id;
		
    select 'Available' into v_product_status
    from homework.sale_fact_table
    where product_key = v_product_key
    and stock >= p_qty;
    
    if v_product_status = 'Available' then
    
        select user_key into v_user_key
    	from homework.dim_user
    	where user_id = p_user_id;
	
	select price into v_price
    	from homework.sale_fact_table
   	where product_key = v_product_key;

	select min(stock) into v_stock
    	from homework.sale_fact_table
	group by product_key
	having product_key = v_product_key;

	insert into homework.dim_date (sale_date, month, year)
	select current_date,
    	extract(month from current_date)::int,
    	extract(year from current_date)::int
	where not exists (select 1 from homework.dim_date where sale_date = current_date);

	select date_key into v_date_key
    	from homework.dim_date
   	where sale_date = current_date;
        
        insert into homework.sale_fact_table (sale_id, product_key, user_key, date_key, price, 
	quantity, total_sale, stock)
	values (p_sale_id, v_product_key, v_user_key, v_date_key, v_price, 
	p_qty, (v_price * p_qty), (v_stock - p_qty));
        
        raise notice 'Sale successfully completed!';
    
    else
        raise notice 'Insufficient Quantity! Check inventory.';
        
    end if;
end;
$$

call sales_update(8, 4, 10, 2);
