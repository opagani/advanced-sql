-- We want to create a daily report to track:
-- Total unique sessions
-- The average length of sessions in seconds
-- The average number of searches completed before displaying a recipe 
-- The ID of the recipe that was most viewed 


-- clear cache to run profile query
-- alter session set use_cached_result=false;


-- select * from vk_data.events.website_activity limit 1000


-- parse json event_details column fields into recipe_id and event_type 
with events as (

    select
    	event_id,
        session_id,
        event_timestamp,
        trim(parse_json(event_details):"recipe_id", '"') as recipe_id,
        trim(parse_json(event_details):"event", '"') as event_type
    from vk_data.events.website_activity
    group by 1, 2, 3, 4, 5

),

-- select * from events


-- group sessions by session_id
group_sessions as (

	select
    	session_id,
        min(event_timestamp) as min_event_timestamp,
        max(event_timestamp) as max_event_timestamp,
        iff(count_if(event_type = 'view_recipe') = 0, null,
        	round(count_if(event_type = 'search')/count_if(event_type = 'view_recipe'))) as searches_per_recipe_view
    from events
    group by session_id

),

-- select * from group_sessions


-- tracks favorite recipe as total view
favorite_recipe as (

	select
    	date(event_timestamp) as event_day,
        recipe_id,
        count(*) as total_views
    from events
    where recipe_id is not null
    group by 1, 2
    qualify row_number() over (partition by event_day order by total_views desc) = 1
    
),

-- select * from favorite_recipe


-- tracks total unique sessions
-- average length of sessions in seconds
-- average number of searches completed before displaying a recipe 
-- ID of the recipe that was most viewed 
result as (

	select
    	date(min_event_timestamp) as event_day,
        count(session_id) as total_sessions,
        round(avg(datediff('sec', min_event_timestamp, max_event_timestamp))) as avg_session_length_sec,
        max(searches_per_recipe_view) as avg_searches_per_recipe_view,
        max(recipe_name) as favorite_recipe,
        recipe_id
    from group_sessions as gp
    inner join favorite_recipe as fr on date(gp.min_event_timestamp) = fr.event_day
    inner join vk_data.chefs.recipe using (recipe_id)
    group by 1, recipe_id

)

select * from result
order by total_sessions


-- Results:  Analizing the Query Profile

-- It takes about 690 ms to complete the query
-- The most expensive nodes are:

--       Sort:  
--        	From the group_sessions CTE tracking searches_per_recipe_view:  45.5%

--            iff(count_if(event_type = 'view_recipe') = 0, null,
--        	  round(count_if(event_type = 'search')/count_if(event_type = 'view_recipe'))) as searches_per_recipe_view

--       Join:
--          From the result CTE joining by recipe_id with the Chefs.recipe table:  9.1%

--            inner join vk_data.chefs.recipe using (recipe_id)

--        Window_function:
--           From the favorite_recipe table:  9.1%

--              qualify row_number() over (partition by event_day order by total_views desc) = 1         
            
