{{ config(materialized='table') }}

with events as (

    select * from {{ ref('stg_campaign_events') }}

)

select 
    date, 
    session_medium,
    session_source,
    festival_id,
    sum(if(event_name='festival_view', event_count, null)) as festival_views,
    sum(if(event_name='random_button', event_count, null)) as random_button_clicks,
    sum(if(event_name='video_play', event_count, null)) as video_plays,
    sum(if(event_name='tickets_button', event_count, null)) as tickets_button_clicks
from events
where festival_id != NULL or festival_id != '(not set)'
group by date, 
session_medium,
    session_source,
    festival_id