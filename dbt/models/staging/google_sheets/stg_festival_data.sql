{{ config(materialized='table') }}

with source as (
    
    select * from {{ source('marketing_google_sheets', 'festival_data') }}
    
)

select 
    festival_id,
    festival_name,
    festival_country,
    festival_city
from source
