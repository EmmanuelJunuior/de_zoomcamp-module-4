with source as (
    select *
    from `your_project_id.nytaxi.green_tripdata`
)

select
    vendorid as vendor_id,
    lpep_pickup_datetime as pickup_datetime,
    lpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid as rate_code_id,
    store_and_fwd_flag,
    pulocationid as pickup_location_id,
    dolocationid as dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount
from source
