{{ config(materialized='table') }}

with campaign_events as (

    select * from {{ ref('stg_campaign_events') }}

)

select 
    *
from campaign_events
