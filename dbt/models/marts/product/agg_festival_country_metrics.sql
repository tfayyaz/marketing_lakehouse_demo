{{ config(materialized='table') }}

with campaign_events as (

    select * from {{ ref('fct_campaign_events') }}

),
festivals as (

    select * from {{ ref('dim_festival') }}

)

select 
    date, 
    f.festival_country as festival_country,
    f.festival_city as festival_city,
    sum(festival_views) as festival_views,
    sum(video_plays) as video_plays,
    sum(tickets_link_clicked) as tickets_link_clicked
from campaign_events as e
inner join festivals as f
on e.festival_id = f.festival_id
group by date, festival_country, festival_city