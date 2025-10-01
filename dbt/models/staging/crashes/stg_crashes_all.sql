with source as (
    -- Step 1: Source all crash data from crashes_all table
    select * 
    from {{ source('crashes', 'crashes_all') }}
),

selected as (
    -- Step 2: Filter out columns that aren't useful (e.g. deleted records and records that don't have a unique identifer or time)
    select *
    from source
    where isdeleted = false
        and collision_id is not null 
        and crash_date_time is not null
        and latitude != 0 
        and longitude != 0
        and is_preliminary = false
),

latest_version as (
    -- Step 3: Filter out multiple versions of the same crash data keeping only the latest
    select *, row_number() over (partition by collision_id order by report_version desc) as version_rank
    from selected
),

filtered as (
    -- Step 4: Apply filtering to our table to keep only the latest version of each crash record
    select *
    from latest_version
    where version_rank = 1
),

cleaned as (
    -- Step 5: Clean up each column (e.g. standardize fields)
    select 
        collision_id,
        report_number,
        report_version,

        {{is_preliminary('is_preliminary')}} as preliminary_status,

        ncic_code,

        DATE(crash_date_time) as crash_date,
        
        crash_time_description as crash_time,
        
        coalesce(initcap(lower(beat)), 'Unknown') as beat,

        city_id,
        city_code,

        coalesce(initcap(lower(city_name)), 'Unknown') as city_name,

        county_code,

        {{ boolean_to_yesno('city_is_active') }} as is_city_active,

        {{ boolean_to_yesno('city_is_incorporated') }} as is_city_incorporated,

        {{ collision_description('collision_type_description') }} as collision_description_1,
        
        coalesce(initcap(lower(collision_type_other_desc)), 'Unknown') as collision_description_2,
        
        initcap(lower(day_of_week)) as day_of_week,

        {{ dispatch_notified('dispatchnotified') }} as is_dispatch_notified,

        {{ hit_and_run('hitrun') }} as hit_and_run,

        {{ boolean_to_yesno('isdeleted') }} as is_deleted,

        {{ boolean_to_yesno('ishighwayrelated') }} as is_highway_related,
        
        {{ boolean_to_yesno('istowaway') }} as is_tow_away,

        coalesce(initcap(lower(judicialdistrict)), 'Unknown') as judicial_district,

        {{ motor_vehicle_description('motorvehicleinvolvedwithdesc') }} as motor_vehicle_involved_with_crash_description_1,

        coalesce(initcap(lower(motorvehicleinvolvedwithotherdesc)), 'Unknown') as motor_vehicle_involved_with_crash_description_2,

        numberinjured as number_injured,
        numberkilled as number_killed,

        {{ standardize_weather('weather_1') }} as weather_1,

        {{ standardize_weather('weather_2') }} as weather_2,

        {{ standardize_road_condition('road_condition_1') }} as road_condition_1,

        {{ standardize_road_condition('road_condition_2') }} as road_condition_2,
                
        {{ lighting_description('lightingdescription') }} as lighting_description,

        latitude,
        longitude,

        {{ pedestrian_action_description('pedestrianactiondesc') }} as pedestrian_action_description,

        {{ primary_collision_factor('primary_collision_factor_code') }} as primary_collision_factor_description,

        primary_collision_factor_violation,

        primarycollisionpartynumber as primary_collision_party_number,

        initcap(lower(primaryroad)) as primary_road,

        {{ roadway_surface('roadwaysurfacecode') }} as roadway_surface_description,
        
        initcap(lower(secondaryroad)) as secondary_road,

        {{ traffic_control_device('trafficcontroldevicecode') }} as traffic_control_device_description,

        {{ boolean_to_yesno('iscountyroad') }} as is_county_road,

        {{ boolean_to_yesno('isfreeway') }} as is_freeway

    from filtered
),
additional as (
    select *,
        case 
            when number_killed > 0 then 'Fatal'
            else 'Nonfatal'
        end as crash_fatal_nonfatal
    from cleaned
)

select * from additional