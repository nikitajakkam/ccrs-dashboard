{{ 
    config(materialized='table') 
}}

with fact as (
    select * 
    from {{ ref('stg_crashes_all') }}
),

dim_date as (
    select 
        crash_date,
        extract(quarter from crash_date) as crash_quarter
    from {{ ref('dim_date') }}
),

dim_county as (
    select
        dhcs_county_code,
        county_name,
        county_region_description,
        north_south_indicator
    from {{ ref('dhcs_county_code_reference') }}
)

select
    f.collision_id,
    f.ncic_code,
    f.crash_date,
    f.crash_time,
    f.beat,
    f.city_name,
    f.county_code,
    f.is_city_active,
    f.is_city_incorporated,
    f.collision_description_1,
    f.day_of_week,
    f.is_dispatch_notified,
    f.hit_and_run,
    f.is_highway_related,
    f.is_tow_away,
    f.judicial_district,
    f.motor_vehicle_involved_with_crash_description_1,
    f.number_injured,
    f.number_killed,
    f.weather_1,
    f.weather_2,
    f.road_condition_1,
    f.lighting_description,
    f.latitude,
    f.longitude,
    f.pedestrian_action_description,
    f.primary_collision_factor_description,
    f.primary_road,
    f.roadway_surface_description,
    f.secondary_road,
    f.traffic_control_device_description,
    f.is_county_road,
    f.is_freeway,
    d.crash_quarter,
    c.county_name,
    c.county_region_description,
    c.north_south_indicator
from fact f
left join dim_date d
    on f.crash_date = d.crash_date
left join dim_county c
    on f.county_code = c.dhcs_county_code
