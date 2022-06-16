{{ config(materialized='table') }}

with events as (

    select * from {{ ref('fct_events') }}

),
festivals as (

    select * from {{ ref('dim_festival') }}

)

select 
    date, 
    f.festival_country as festival_country,
    f.festival_city as festival_city,
    sum(if(event_name='festival_view', event_count, null)) as festival_views,
    sum(if(event_name='video_play', event_count, null)) as video_plays,
    sum(if(event_name='tickets_link', event_count, null)) as tickets_link_clicked
from events as e
inner join festivals as f
on e.festival_id = f.festival_id
group by date, festival_country, festival_city