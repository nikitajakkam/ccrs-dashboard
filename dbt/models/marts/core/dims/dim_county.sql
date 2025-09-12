{{ config(
    materialized='table'
) }}


select
    dhcs_county_code,
    county_name,
    county_region_code,
    county_region_description,
    fips_county_code,
    fips_state_county_code,
    north_south_indicator
from
    {{ 
        ref('dhcs_county_code_reference') 
    }}