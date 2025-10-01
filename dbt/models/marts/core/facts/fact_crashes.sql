{{ config(
    materialized='table'
) }}

select
    collision_id,
    ncic_code,
    crash_date,
    crash_time,
    beat,
    city_name,
    county_code,
    is_city_active,
    is_city_incorporated,
    collision_description_1,
    day_of_week,
    is_dispatch_notified,
    hit_and_run,
    is_highway_related,
    is_tow_away,
    judicial_district,
    motor_vehicle_involved_with_crash_description_1,
    number_injured,
    number_killed,
    weather_1,
    weather_2,
    road_condition_1,
    lighting_description,
    latitude,
    longitude,
    pedestrian_action_description,
    primary_collision_factor_description,
    primary_road,
    roadway_surface_description,
    secondary_road,
    traffic_control_device_description,
    is_county_road,
    is_freeway,
    crash_fatal_nonfatal

from {{ ref('stg_crashes_all') }}
