{% macro motor_vehicle_description(column_name) %}
case
    when lower(trim({{ column_name }})) in ('other motor vehicle') then 'Other Motor Vehicle'
    when lower(trim({{ column_name }})) in ('fixed object') then 'Fixed Object'
    when lower(trim({{ column_name }})) in ('parked motor vehicle') then 'Parked Motor Vehicle'
    when lower(trim({{ column_name }})) in ('non-collision') then 'Non-Collision'
    when lower(trim({{ column_name }})) in ('other object') then 'Other Object'
    when lower(trim({{ column_name }})) in ('pedestrian') then 'Pedestrian'
    when lower(trim({{ column_name }})) in ('bicycle') then 'Bicycle'
    when lower(trim({{ column_name }})) in ('animal') then 'Animal'
    when lower(trim({{ column_name }})) in ('train') then 'Train'
    when lower(trim({{ column_name }})) in ('motor vehicle on other roadway') then 'Motor Vehicle On Other Roadway'
    else 'Unknown'
end
{% endmacro %}
