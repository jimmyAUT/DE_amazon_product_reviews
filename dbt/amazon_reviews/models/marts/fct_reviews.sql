{{ 
  config(
    materialized='incremental',
    unique_key='review_id',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "review_month", 
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by=["user_id", "asin"]
  ) 
}}

with reviews as (
    select * from {{ ref('stg_reviews') }}
),

dim_users as (
    select * from {{ ref('dim_users') }}
),

dim_items as (
    select * from {{ ref('dim_items') }}
),

dim_time as (
    select * from {{ ref('dim_time') }}
),

-- product average review rating
item_avg as (
    select
        asin,
        avg(rating) as avg_rating
    from reviews
    group by asin
),

final as (
    select
        review_id,
        -- transform review_id to string for power bi use
        to_hex(r.review_id) as review_id_power_bi,

        r.user_id,
        r.asin,
        r.rating,
        r.verified_purchase,
        r.helpful_votes,
        r.timestamp,
        TIMESTAMP_TRUNC(r.timestamp, MONTH) as review_month,


        di.price,
        di.category,
        di.store,

        ia.avg_rating,
        -- rating difference from average rating
        round(r.rating - ia.avg_rating, 2) as rating_diff_from_avg,

        dt.date_day as review_date,
        dt.day_name


    from reviews r
    left join dim_time dt on DATE(r.timestamp) = dt.date_day
    left join dim_users du on r.user_id = du.user_id
    left join dim_items di on r.asin = di.asin
    left join item_avg ia on r.asin = ia.asin
)

select * from final