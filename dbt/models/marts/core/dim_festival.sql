{{ config(materialized='table') }}

with festival_data as (

    select * from {{ ref('stg_festival_data') }}

)

select 
    *
from festival_data
