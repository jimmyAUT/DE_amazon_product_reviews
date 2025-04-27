{{ config(
    materialized='table' 
) }}

with source as (
    select * from {{ ref('stg_items') }}
)

select
    asin,
    title,
    category,
    store,
    price,
    average_rating,
    rating_number
from source
