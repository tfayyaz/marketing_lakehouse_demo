{{ config(materialized='table') }}

with events as (

    select * from {{ ref('fct_events') }}

)

select 
    date, 
    session_source as source, 
    session_medium as medium,
    sum(if(event_name='festival_view', event_count, null)) as festival_views,
    sum(if(event_name='video_play', event_count, null)) as video_plays,
    sum(if(event_name='tickets_link', event_count, null)) as tickets_link_clicked
from events
group by date, source, medium