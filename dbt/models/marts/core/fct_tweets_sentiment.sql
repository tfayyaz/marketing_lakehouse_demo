{{ config(materialized='table') }}

with tweets as (

    select * from {{ ref('stg_tweets_sentiment') }}

)

select
    festival_id,
    text,
    sentiment,
    if(sentiment = 'positive', 1, 0) as positive,
    if(sentiment = 'negative', 1, 0) as negative
from tweets