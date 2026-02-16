with yellow as (
    select * from {{ ref('stg_yellow_tripdata') }}
),
green as (
    select * from {{ ref('stg_green_tripdata') }}
)

select * from yellow
union all
select * from green
