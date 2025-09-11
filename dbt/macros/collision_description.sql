{% macro collision_description(column_name) %}
case
    when {{ column_name }} is null then 'Unknown'
    when lower(trim({{ column_name }})) in ('rear end', 'rear-end') then 'Rear End'
    when lower(trim({{ column_name }})) in ('side swipe', 'sideswipe', 'side-swip') then 'Side Swipe'
    when lower(trim({{ column_name }})) in ('hit object', 'hit-object') then 'Hit Object'
    when lower(trim({{ column_name }})) = 'broadside' then 'Broadside'
    when lower(trim({{ column_name }})) = 'head-on' then 'Head-On'
    when lower(trim({{ column_name }})) = 'overturned' then 'Overturned'
    when lower(trim({{ column_name }})) = 'other' then 'Other'
    when lower(trim({{ column_name }})) in ('vehicle/pedestrian', 'vehicle pedestrian', 'vehicle-pedestrian', 'vehicle/pedestrain', 'vehicle pedestrain', 'vehicle-pedestrain') then 'Vehicle/Pedestrian'
    else 'Unknown'
end
{% endmacro %}
