/*
  Oscar Pagani - Advanced SQL 
  Project 1 - Exercise 1

  For our first exercise, we need to determine which customers are eligible to order from Virtual Kitchen, 
  and which distributor will handle the orders that they place. We want to return the following information:

    Customer ID
    Customer first name
    Customer last name
    Customer email
    Supplier ID
    Supplier name
    Shipping distance in kilometers or miles (you choose)
*/

-- If the customer is able to order from us, then their city/state will be present in our database. 
-- Create a query in Snowflake that returns all customers that can place an order with Virtual Kitchen.

with cities as (
    -- removing duplicate city/state combination
    select
        upper(trim(city_name)) as city_name,
        upper(state_abbr) as state_name,
        lat,
        long
    from vk_data.resources.us_cities
    -- use qualify to directly know what row number are you intended to fiter
    -- if a combination city/state has 3 rows (1, 2, and 3) you will select the row number = 1
    qualify row_number() over(partition by city_name, state_abbr order by city_name) = 1
)

-- select * from cities

-- Use the customer's city and state to join the us_cities resources table
, customers as (
    select 
        c1.customer_id,
        c1.first_name as customer_first_name,
        c1.last_name as customer_last_name,
        c1.email as customer_email,
    c2.customer_city,
    c2.customer_state,
    cities.lat as customer_lat,
    cities.long as customer_long
    from vk_data.customers.customer_data as c1
    inner join vk_data.customers.customer_address c2 using(customer_id)
    inner join cities on (
      upper(trim(c2.customer_city)) = upper(cities.city_name)
      and upper(trim(c2.customer_state)) = upper(cities.state_name)
    )
)

-- select * from customers

, suppliers as (
    select
        supplier_id,
        supplier_name,
        upper(trim(supplier_city)) as supplier_city,
        upper(supplier_state) as supplier_state,
        cities.lat as supplier_lat,
        cities.long as supplier_long
    from vk_data.suppliers.supplier_info as s1
    left join cities on
            upper(s1.supplier_city) = cities.city_name
            and upper(s1.supplier_state) = cities.state_name
)

-- select * from suppliers limit 10 

, final_result as (
    select
        customer_id
        customer_first_name,
        customer_last_name,
        customer_email,
        supplier_id,
        supplier_name,
        st_distance(st_makepoint(customers.customer_long, customers.customer_lat),
                    st_makepoint(suppliers.supplier_long, suppliers.supplier_lat)) / 1000 as distance_in_km
    from customers
    cross join suppliers
    qualify row_number() over(partition by customer_id order by distance_in_km) = 1
    order by customer_last_name, customer_first_name
)

select * from final_result;