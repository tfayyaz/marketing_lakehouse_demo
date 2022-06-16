{{ config(materialized='table') }}

with source as (
    
    select * from {{ source('marketing_google_analytics_4', 'campaign_events') }}
    
)

select 
    date,	
    session_medium,
    session_source, 
    event_name,	
    custom_event_festival_id as festival_id,
    custom_event_festival_name as festival_name,
    custom_event_festival_video_id as festival_video_id,
    sessions,
    total_users,
    event_count
from source