{{ 
    config(materialized='table') 
}}

select distinct
    crash_date,
    crash_time,
    extract(year from crash_date) as crash_year,
    extract(quarter from crash_date) as crash_quarter,
    extract(hour from crash_time) as crash_hour
from
    {{ 
        ref('stg_crashes_all') 
    }}