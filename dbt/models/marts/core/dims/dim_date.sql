{{ config(
    materialized='table'
) }}

select distinct
    crash_date,
    extract(quarter from crash_date) as crash_quarter,
from
    {{ 
        ref('stg_crashes_all') 
    }}