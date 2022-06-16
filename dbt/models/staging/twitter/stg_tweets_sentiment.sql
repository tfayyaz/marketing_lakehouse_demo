{{ config(materialized='table') }}

with source as (
    
    select * from {{ source('marketing_twitter', 'tweets_sentiment') }}
    
)

select 
    festival_id,
    text,
    sentiment
from source