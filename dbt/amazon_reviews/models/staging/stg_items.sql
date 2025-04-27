{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('staging', 'stg_items_ext') }}
),

renamed as (
    select
        parent_asin as asin,
        main_category as category,
        title,
        store,

        -- clean the price to pure number form
        SAFE_CAST(
            regexp_extract(price, r'\d+(?:\.\d+)?') AS FLOAT64
        ) as price,
        SAFE_CAST(average_rating AS FLOAT64) as average_rating,
        SAFE_CAST(rating_number AS FLOAT64) as rating_number
    from source
)

select * from renamed
