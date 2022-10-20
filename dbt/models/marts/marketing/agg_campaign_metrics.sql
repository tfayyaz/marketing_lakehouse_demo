{{ config(materialized='table') }}

with campaign_events as (

    select * from {{ ref('fct_campaign_events') }}

)

select 
    date, 
    session_source as source, 
    session_medium as medium,
    sum(festival_views) as festival_views,
    sum(video_plays) as video_plays,
    sum(tickets_button_clicks) as tickets_button_clicks
from campaign_events
group by date, source, medium