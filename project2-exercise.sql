-- Initial query to be reworked

-- select 
--     first_name || ' ' || last_name as customer_name,
--     ca.customer_city,
--     ca.customer_state,
--     s.food_pref_count,
--     (st_distance(us.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
--     (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
-- from vk_data.customers.customer_address as ca
-- join vk_data.customers.customer_data c on ca.customer_id = c.customer_id
-- left join vk_data.resources.us_cities us 
-- on UPPER(rtrim(ltrim(ca.customer_state))) = upper(TRIM(us.state_abbr))
--     and trim(lower(ca.customer_city)) = trim(lower(us.city_name))
-- join (
--     select 
--         customer_id,
--         count(*) as food_pref_count
--     from vk_data.customers.customer_survey
--     where is_active = true
--     group by 1
-- ) s on c.customer_id = s.customer_id
--     cross join 
--     ( select 
--         geo_location
--     from vk_data.resources.us_cities 
--     where city_name = 'CHICAGO' and state_abbr = 'IL') chic
-- cross join 
--     ( select 
--         geo_location
--     from vk_data.resources.us_cities 
--     where city_name = 'GARY' and state_abbr = 'IN') gary
-- where 
--     ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%')
--     and customer_state = 'KY')
--     or
--     (customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
--     or
--     (customer_state = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%')



-- Solution

-- first we join each customer with his city and state
with customer_name_city_state as (
    select
        first_name || ' ' || last_name as customer_name,
        ca.customer_id,
        ca.customer_city,
        ca.customer_state
    from vk_data.customers.customer_address as ca
    join vk_data.customers.customer_data as c on ca.customer_id = c.customer_id
),

-- select * from customer_name_city_state

-- join the resource us cities per customer
resource_cities_per_customer as (
    select
        *
    from customer_name_city_state as c
    left join vk_data.resources.us_cities us 
        on UPPER(rtrim(ltrim(c.customer_state))) = upper(TRIM(us.state_abbr))
        and trim(lower(c.customer_city)) = trim(lower(us.city_name))
),

-- select * from resources_cities_per_customer

-- select food preferences count per customer
count_food_preferences_per_customer as (
    select 
        customer_id,
        count(*) as food_pref_count
    from vk_data.customers.customer_survey as cs
    where is_active = true
    group by customer_id
),

-- select * from count_food_preferences_per_customer

-- select the geo_location from all the cities in Chicago, Illinois
chicago_customer_geolocation as (
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
),

-- select * from chicago_customer_geolocation

-- select the geo_location from all the cities in Gary, Indiana
gary_customer_geolocation as (
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
)

-- select * from gary_customer_geolocation


-- final query
select 
    r.customer_name,
    r.customer_city,
    r.customer_state,
    s.food_pref_count,
    (st_distance(r.geo_location, chic.geo_location) / 1609)::int as chicago_distance_miles,
    (st_distance(r.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
from resource_cities_per_customer as r
join count_food_preferences_per_customer as s on r.customer_id = s.customer_id
cross join chicago_customer_geolocation as chic
cross join gary_customer_geolocation as gary
where 
    ((trim(city_name) ilike '%concord%' or trim(city_name) ilike '%georgetown%' or trim(city_name) ilike '%ashland%')
    and r.customer_state = 'KY')
    or
    (r.customer_state = 'CA' and (trim(city_name) ilike '%oakland%' or trim(city_name) ilike '%pleasant hill%'))
    or
    (r.customer_state = 'TX' and (trim(city_name) ilike '%arlington%') or trim(city_name) ilike '%brownsville%')
