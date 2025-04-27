-- models/staging/stg_reviews.sql
{{
  config(
    materialized='view'
  )
}}

with source as (
  select 
    user_id,
    parent_asin,
    rating,
    verified_purchase,
    helpful_votes,
    timestamp,
    year,
    month,
    day
  from {{ source('staging', 'stg_reviews_ext') }}
  where rating is not null and timestamp is not null
)

select
  -- 生成 review_id
  MD5(CONCAT(
              COALESCE(CAST(user_id AS STRING), ""),
              COALESCE(CAST(parent_asin AS STRING), ""),
              COALESCE(CAST(timestamp AS STRING), ""))) as review_id,
  parent_asin as asin,
  user_id,
  rating,
  verified_purchase,
  helpful_votes,
  timestamp,
  year,
  month,
  day
from source
