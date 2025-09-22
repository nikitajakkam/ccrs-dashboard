{{ config(
    materialized='table'
) }}


select
    dhcs_county_code,
    case 
        when county_name is not null then concat(county_name, ' County')
        else null
    end as county_name,
    county_region_code,
    county_region_description,
    fips_county_code,
    case
        when fips_state_county_code is not null then lpad(cast(fips_state_county_code as string), 5, '0')
        else null
    end as fips_code,
    north_south_indicator
from
    {{ 
        ref('dhcs_county_code_reference') 
    }}