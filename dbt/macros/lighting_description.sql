{% macro lighting_description(column_name) %}
case
    when {{ column_name }} is null then 'Unknown'
    when lower(trim({{ column_name }})) in ('daylight') then 'Daylight'
    when lower(trim({{ column_name }})) in ('dark-street lights', 'dark - street lights') then 'Dark - Street Lights'
    when lower(trim({{ column_name }})) in ('dark-no street lights', 'dark - no street lights') then 'Dark - No Street Lights'
    when lower(trim({{ column_name }})) in ('dusk-dawn') then 'Dusk/Dawn'
    when lower(trim({{ column_name }})) in ('dark-street lights not functioning*', 'dark - street lights not functioning') then 'Dark - Street Lights Not Functioning'
    else 'Unknown'
end
{% endmacro %}
