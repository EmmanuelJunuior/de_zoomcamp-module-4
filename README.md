# de_zoomcamp-module-4
Zoomcamp Module 4
This repository contains the solution for **Module 4: Analytics Engineering** from the **Data Engineering Zoomcamp 2026**.

**Homework Answers**
Question 1 – dbt Lineage

dbt run --select int_trips_unioned builds:
✅ **stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned**

Question 2 – dbt Test on New Payment Type

A new value 6 appears in payment_type. Running dbt test --select fct_trips:
✅ **dbt fails the test with non-zero exit code**

Question 3 – Record Count

Number of records in fct_monthly_zone_revenue:
✅ **14,120**

Question 4 – Highest Revenue Zone (Green Taxis 2020)

Pickup zone with highest total revenue (revenue_monthly_total_amount) for Green taxis in 2020:
✅ **East Harlem North**

Question 5 – Green Taxi Trips in October 2019

Total trips (total_monthly_trips) for Green taxis in October 2019:
✅ **384,624**

Question 6 – FHV Staging Model Record Count

Number of records in stg_fhv_tripdata after filtering dispatching_base_num IS NOT NULL:
✅ **43,244,693**


## Overview

The goal of this module is to transform NYC taxi data (yellow, green, and FHV trips for 2019-2020) into analytics-ready models using **dbt**.

### Features

- Staging models for raw taxi data (`stg_yellow_tripdata`, `stg_green_tripdata`, `stg_fhv_tripdata`)
- Intermediate model to union green and yellow taxi trips (`int_trips_unioned`)
- Fact table for monthly revenue and trip counts per zone (`fct_monthly_zone_revenue`)
- Cross-database compatible macros for trip duration and vendor mapping
- Schema and tests defined in `schema.yml`

## Requirements

- dbt Cloud or dbt Core
- BigQuery account with `nytaxi` dataset loaded for 2019-2020
- Service account with **BigQuery Data Editor, Job User, and User roles**

## How to Run

```bash
# Install dependencies
dbt deps

# Build all models in the production environment
dbt build --target prod

# Run tests
dbt test --target prod


---

### **2. dbt_project.yml**

```yaml
name: 'de-zoomcamp-module-4'
version: '1.0'
config-version: 2

profile: 'default'

source-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
macro-paths: ["macros"]
data-paths: ["data"]
target-path: "target"
clean-targets:
  - "target"
  - "dbt_modules"

models:
  de-zoomcamp-module-4:
    staging:
      materialized: view
    intermediate:
      materialized: view
    marts:
      materialized: table
```
3. Staging Models
stg_yellow_tripdata.sql

```
with source as (
    select *
    from `adebimpe-de-zoomcamp.nytaxi.yellow_tripdata`
)

select
    vendorid as vendor_id,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
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
    total_amount,
    'yellow' as taxi_type
from source
```

stg_green_tripdata.sql

```
with source as (
    select *
    from `adebimpe-de-zoomcamp.nytaxi.green_tripdata`
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
    total_amount,
    'green' as taxi_type
from source
```

stg_fhv_tripdata.sql

```
with source as (
    select *
    from `adebimpe-de-zoomcamp.nytaxi.fhv_tripdata_2019`
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
```

4. Intermediate Model
int_trips_unioned.sql

```
with yellow as (
    select * from {{ ref('stg_yellow_tripdata') }}
),
green as (
    select * from {{ ref('stg_green_tripdata') }}
)

select * from yellow
union all
select * from green
```

5. Fact Model
fct_monthly_zone_revenue.sql


```
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
    count(*) as total_monthly_trips,
    sum(revenue_monthly_total_amount) as revenue_monthly_total_amount
from trips
group by year, month, pickup_location_id, taxi_type
```

6. Macros
get_trip_duration_minutes.sql

```
{% macro get_trip_duration_minutes(pickup_datetime, dropoff_datetime) %}
    {{ dbt.datediff(pickup_datetime, dropoff_datetime, 'minute') }}
{% endmacro %}
```

get_vendor_data.sql

```
{% macro get_vendor_data(vendor_id_column) %}

{% set vendors = {
    1: 'Creative Mobile Technologies',
    2: 'VeriFone Inc.',
    4: 'Unknown/Other'
} %}

case {{ vendor_id_column }}
    {% for vendor_id, vendor_name in vendors.items() %}
    when {{ vendor_id }} then '{{ vendor_name }}'
    {% endfor %}
end

{% endmacro %}
```

safe_cast.sql

```{% macro safe_cast(column, data_type) %}
    {% if target.type == 'bigquery' %}
        safe_cast({{ column }} as {{ data_type }})
    {% else %}
        cast({{ column }} as {{ data_type }})
    {% endif %}
{% endmacro %}
```

7. schema.yml
```
version: 2

models:
  - name: fct_monthly_zone_revenue
    description: "Fact table of monthly revenue by pickup zone and taxi type"
    columns:
      - name: year
        description: "Year of the trip"
      - name: month
        description: "Month of the trip"
      - name: pickup_location_id
        description: "Pickup location zone ID"
      - name: taxi_type
        description: "Green or Yellow taxi"
      - name: total_monthly_trips
        description: "Total number of trips per month"
      - name: revenue_monthly_total_amount
        description: "Total revenue per month"

sources:
  - name: nytaxi
    description: "Raw NYC taxi data 2019-2020"
    tables:
      - name: green_tripdata
      - name: yellow_tripdata
      - name: fhv_tripdata_2019
```
