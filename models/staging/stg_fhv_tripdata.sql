with source as (
    select *
    from `your_project_id.nytaxi.fhv_tripdata_2019`
    where dispatching_base_num is not null
)

select
    dispatching_base_num as base_num,
    pickup_datetime,
    dropoff_datetime,
    pu_location_id as pickup_location_id,
    do_location_id as dropoff_location_id,
    passenger_count,
    trip_distance
from source
