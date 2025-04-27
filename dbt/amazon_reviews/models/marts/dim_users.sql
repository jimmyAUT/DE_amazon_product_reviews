{{ config(
    materialized='table' 
) }}

with reviews as (
    select * from {{ ref('stg_reviews') }}
),

items as (
    select asin, price from {{ ref('dim_items') }}
),

reviews_with_price as (
    select
        r.*,
        i.price
    from reviews r
    left join items i on r.asin = i.asin
)

select
    user_id,
    countif(verified_purchase = true) as verified_purchase_count,
    count(*) as review_count,

    -- first and last review date
    min(timestamp) as first_review_date,
    max(timestamp) as last_review_date,
        
    -- active review days
    count(distinct DATE(timestamp)) as active_days,

    -- average review time span
    case 
        when count(*) > 1 then 
            date_diff(max(DATE(timestamp)), min(DATE(timestamp)), day) / (count(*) - 1)
        else null 
    end as avg_days_between_reviews,
    sum(coalesce(price, 0)) as total_spent -- in case price is null
from reviews_with_price
where user_id is not null
group by user_id
