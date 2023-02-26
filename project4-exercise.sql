-- Part 2: Review the candidate's tech exercise below, and provide a one-paragraph assessment of the SQL quality. 
-- Provide examples/suggestions for improvement if you think the candidate could have chosen a better approach.

-- Do you agree with the results returned by the query?
-- YES

-- Is it easy to understand?
-- Not so easy

-- Could the code be more efficient?
-- Yes


--Candidate query
-- with urgent_orders as (
--     select
--     	o_orderkey,
--     	o_orderdate,
--         c_custkey,
--         p_partkey,
--         l_quantity,
--         l_extendedprice,
--         row_number() over (partition by c_custkey order by l_extendedprice desc) as price_rank
--     from snowflake_sample_data.tpch_sf1.orders as o
--     inner join snowflake_sample_data.tpch_sf1.customer as c on o.o_custkey = c.c_custkey
--     inner join snowflake_sample_data.tpch_sf1.lineitem as l on o.o_orderkey = l.l_orderkey
--     inner join snowflake_sample_data.tpch_sf1.part as p on l.l_partkey = p.p_partkey
--     where c.c_mktsegment = 'AUTOMOBILE'
--     	and o.o_orderpriority = '1-URGENT'
--     order by 1, 2),

-- top_orders as (
--     select
--     	c_custkey,
--         max(o_orderdate) as last_order_date,
--         listagg(o_orderkey, ', ') as order_numbers,
--         sum(l_extendedprice) as total_spent
--     from urgent_orders
--     where price_rank <= 3
--     group by 1
--     order by 1)

-- select 
-- 	t.c_custkey,
--     t.last_order_date,
--     t.order_numbers,
--     t.total_spent,
--     u.p_partkey as part_1_key,
--     u.l_quantity as part_1_quantity,
--     u.l_extendedprice as part_1_total_spent,
--     u2.p_partkey as part_2_key,
--     u2.l_quantity as part_2_quantity,
--     u2.l_extendedprice as part_2_total_spent,
--     u3.p_partkey as part_3_key,
--     u3.l_quantity as part_3_quantity,
--     u3.l_extendedprice as part_3_total_spent
-- from top_orders as t
-- inner join urgent_orders as u on t.c_custkey = u.c_custkey
-- inner join urgent_orders as u2 on t.c_custkey = u2.c_custkey
-- inner join urgent_orders as u3 on t.c_custkey = u3.c_custkey
-- where u.price_rank = 1 and u2.price_rank = 2 and u3.price_rank = 3
-- order by t.last_order_date desc
-- limit 100



-- Improved more efficient query

with urgent_orders as (
	select
    	customer.c_custkey
        , orders.o_orderkey
        , orders.o_totalprice
        , orders.o_orderdate
        , row_number() over (partition by c_custkey order by o_totalprice desc) as total_price_rank
    from
    	snowflake_sample_data.tpch_sf1.orders
    left join snowflake_sample_data.tpch_sf1.customer 
    	on orders.o_custkey = customer.c_custkey
    where orders.o_orderpriority = '1-URGENT'
    	and customer.c_mktsegment = 'AUTOMOBILE'        
)

-- select * from urgent_orders

, top_order_summary as (
	select
    	c_custkey as c_custkey
        , max(o_orderdate) as last_order_date
        , listagg(o_orderkey, ', ') as order_number
        , sum(o_totalprice) as total_spent
    from urgent_orders
    where total_price_rank <= 3
	group by 1
)

-- select * from top_order_summary
-- select c_custkey, count(*) from top_order_summary group by 1 order by 1 desc
-- select * from snowflake_sample_data.tpch_sf1.lineitem

, top_parts as (
	select
    	urgent_orders.c_custkey
        , lineitem.l_partkey
        , sum(lineitem.l_quantity) as quantity_ordered
        , sum(lineitem.l_extendedprice) as total_spent_on_part
        , row_number() over (partition by urgent_orders.c_custkey order by sum(lineitem.l_extendedprice) desc) as part_rank
    from 
		snowflake_sample_data.tpch_sf1.lineitem 
    inner join urgent_orders
    	on lineitem.l_orderkey = urgent_orders.o_orderkey
    group by 1, 2
    qualify row_number() over (partition by urgent_orders.c_custkey order by sum(lineitem.l_extendedprice) desc) < 4
)

-- select * from top_parts

, customer_parts as (
	select 
    	tos.c_custkey
        , tos.last_order_date
        , tos.order_number
        , tos.total_spent
        , tp.l_partkey
        , tp.quantity_ordered
        , tp.total_spent_on_part
        , tp.part_rank
    from top_order_summary as tos
    left join top_parts as tp on tp.c_custkey = tos.c_custkey
    -- where tos.c_custkey = 65782  
)

-- select * from customer_parts

SELECT
	c_custkey,
    last_order_date,
    order_number,
    total_spent,
    MAX(CASE WHEN part_rank = 1 THEN l_partkey END) as part_1_key,
    MAX(CASE WHEN part_rank = 1 THEN quantity_ordered END) as part_1_quantity,
    MAX(CASE WHEN part_rank = 1 THEN total_spent_on_part END) as part_1_total_spent,
    MAX(CASE WHEN part_rank = 2 THEN l_partkey END) as part_2_key,
    MAX(CASE WHEN part_rank = 2 THEN quantity_ordered END) as part_2_quantity,
    MAX(CASE WHEN part_rank = 2 THEN total_spent_on_part END) as part_2_total_spent,
    MAX(CASE WHEN part_rank = 3 THEN l_partkey END) as part_3_key,
    MAX(CASE WHEN part_rank = 3 THEN quantity_ordered END) as part_3_quantity,
    MAX(CASE WHEN part_rank = 3 THEN total_spent_on_part END) as part_3_total_spent
FROM
	customer_parts
GROUP BY
	c_custkey,
    last_order_date,
    order_number,
    total_spent
ORDER BY 2 DESC, 4 DESC