with trips as (
    select *,
        {{ get_trip_duration_minutes('pickup_datetime', 'dropoff_datetime') }} as trip_duration_minutes,
        {{ get_vendor_data('vendor_id') }} as vendor_name,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        case 
            when fare_amount + tip_amount > 0 then fare_amount + tip_amount
            else total_amount
        end as revenue_monthly_total_amount
    from {{ ref('int_trips_unioned') }}
)

select 
    year,
    month,
    pickup_location_id,
    taxi_type,
    sum(trip_duration_minutes) as total_trip_minutes,
    sum(total_amount) as total_monthly_amount,
    sum(1) as total_monthly_trips,
    sum(revenue_monthly_total_amount) as revenue_monthly_total_amount
from trips
group by year, month, pickup_location_id, taxi_type
