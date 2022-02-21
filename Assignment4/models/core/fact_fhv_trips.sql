{{ config(materialized='table') }}

with fhv_data as (
    select * from {{ ref('staging_fhv2019') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    fhv_data.dispatching_base_num, 
    fhv_data.pickup_datetime, 
    fhv_data.dropoff_datetime,
    fhv_data.DOLocationID, 
    fhv_data.PULocationID,
    fhv_data.SR_Flag, 

from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PULocationID = pickup_zone.LocationID
inner join dim_zones as dropoff_zone
on fhv_data.DOLocationID = dropoff_zone.LocationID

