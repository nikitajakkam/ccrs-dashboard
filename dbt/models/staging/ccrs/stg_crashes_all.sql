with source as (
    -- Step 1: Source all crash data from crashes_all table
    select * 
    from {{ source('ccrs', 'crashes_all') }}
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

        case 
            when is_preliminary = true then 'Preliminary'
            when is_preliminary = false then 'Final'
            else 'Unknown'
        end as preliminary_status,

        ncic_code,

        DATE(crash_date_time) as crash_date,
        
        crash_time_description as crash_time,
        beat,
        city_id,
        city_code,

        initcap(lower(city_name)) as city_name,

        county_code,

        case city_is_active
            when true then 'Yes'
            when false then 'No'
            else 'Unknown'
        end as is_city_active,

        case city_is_incorporated
            when true then 'Yes'
            when false then 'No'
            else 'Unknown'
        end as is_city_incorporated,

        initcap(lower(collision_type_description)) as collision_description,
        
        initcap(lower(collision_type_other_desc)) as collision_secondary_description,
        
        initcap(lower(day_of_week)) as day_of_week,

        case dispatchnotified
            when 'Yes' then 'Yes'
            when 'No' then 'No'
            when 'NotApplicable' then 'Not Applicable'
            else 'Unknown'
        end as dispatch_notified,

        case hitrun
            when 'M' then 'Felony'
            when 'F' then 'Misdemeanor'
            else 'None'
        end as hit_and_run,

        case isdeleted
            when true then 'Yes'
            when false then 'No'
            else 'Unknown'
        end as is_deleted,

        case ishighwayrelated
            when false then 'No'
            when true then 'Yes'
            else 'Unknown'
        end as is_highway_related,

        case istowaway
            when false then 'No'
            when true then 'Yes'
            else 'Unknown'
        end as is_tow_away,

        initcap(lower(judicialdistrict)) as judicial_district,

        initcap(lower(motorvehicleinvolvedwithdesc)) as motor_vehicle_involved_with_crash_description,

        initcap(lower(motorvehicleinvolvedwithotherdesc)) as motor_vehicle_involved_with_crash_secondary_description,

        numberinjured as number_injured,
        numberkilled as number_killed,

        initcap(lower(weather_1)) as weather,

        case 
            when lower(weather_2) in ('hail', 'hailing') then 'Hail'
            when lower(weather_2) in ('smoke', 'smokey') then 'Smoke'
            else initcap(lower(weather_2))
        end as weather_additional,

        case
            when upper(road_condition_1) like 'HOLES%' then 'Holes, Deep Ruts'
            when upper(road_condition_1) like 'LOOSE MATERIAL ON ROADWAY%' then 'Loose Material on Roadway'
            when upper(road_condition_1) like 'OBSTRUCTION ON ROADWAY%' then 'Obstruction on Roadway'
            when upper(road_condition_1) like 'CONSTRUCTION%' then 'Construction or Repair Zone'
            when upper(road_condition_1) like 'REDUCED ROADWAY WIDTH%' then 'Reduced Roadway Width'
            when upper(road_condition_1) like 'FLOODED%' then 'Flooded'
            when upper(road_condition_1) like 'OTHER%' then 'Other'
            when upper(road_condition_1) like 'NO UNUSUAL CONDITION%' then 'No Unusual Conditions'
            else initcap(lower(road_condition_1))
        end as road_condition,
        
        case
            when upper(road_condition_2) like 'HOLES%' then 'Holes, Deep Ruts'
            when upper(road_condition_2) like 'LOOSE MATERIAL ON ROADWAY%' then 'Loose Material on Roadway'
            when upper(road_condition_2) like 'OBSTRUCTION ON ROADWAY%' then 'Obstruction on Roadway'
            when upper(road_condition_2) like 'CONSTRUCTION%' then 'Construction or Repair Zone'
            when upper(road_condition_2) like 'REDUCED ROADWAY WIDTH%' then 'Reduced Roadway Width'
            when upper(road_condition_2) like 'FLOODED%' then 'Flooded'
            when upper(road_condition_2) like 'OTHER%' then 'Other'
            when upper(road_condition_2) like 'NO UNUSUAL CONDITION%' then 'No Unusual Conditions'
            else initcap(lower(road_condition_2))
        end as road_condition_additional,

        initcap(lower(lightingdescription)) as lighting_description,

        latitude,
        longitude,

        initcap(lower(pedestrianactiondesc)) as pedestrian_action_description,

        case primary_collision_factor_code
            when 'A' then '(Vehicle) Code Violation'
            when 'B' then 'Other Improper Driving'
            when 'C' then 'Other Than Driver'
            when 'D' then 'Unknown'
            when 'E' then 'Fell Asleep'
            when null then 'Not Stated'
            else initcap(lower(primary_collision_factor_code))  
        end as primary_collision_factor_code,

        primary_collision_factor_violation,

        primarycollisionpartynumber as primary_collision_party_number,
        primaryroad as primary_road,

        case roadwaysurfacecode
            when 'A' then 'Dry'
            when 'B' then 'Wet'
            when 'C' then 'Snowy/Icy'
            when 'D' then 'Slippery (Mud, Oil, etc)'
            when null then 'Not Stated'
            else initcap(lower(roadwaysurfacecode))
        end as roadway_surface_description,
        
        initcap(lower(secondaryroad)) as secondary_road,

        case trafficcontroldevicecode
            when 'A' then 'Controls Functioning'
            when 'B' then 'Controls Not Functioning'
            when 'C' then 'Controls Obscured'
            when 'D' then 'No Controls Present/Factor'
            when null then 'Not Stated'
            else initcap(lower(trafficcontroldevicecode))
        end as traffic_control_device_description,

        case iscountyroad
            when false then 'No'
            when true then 'Yes'
            else 'Unknown'
        end as is_county_road,

        case isfreeway
            when false then 'No'
            when true then 'Yes'
            else 'Unknown'
        end as is_freeway,

    from filtered
)

select * from cleaned