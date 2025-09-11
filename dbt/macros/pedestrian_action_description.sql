{% macro pedestrian_action_description(column_name) %}
case
    when {{ column_name }} is null then 'Unknown'
    when lower(trim({{ column_name }})) = 'no pedestrians involved' then 'No Pedestrians Involved'
    when lower(trim({{ column_name }})) = 'crossing in cross walk at intersection' then 'Crossing In Cross Walk At Intersection'
    when lower(trim({{ column_name }})) = 'in road - includes shoulder' then 'In Road - Includes Shoulder'
    when lower(trim({{ column_name }})) = 'crossing - not in crosswalk' then 'Crossing - Not In Crosswalk'
    when lower(trim({{ column_name }})) = 'not in road' then 'Not In Road'
    when lower(trim({{ column_name }})) = 'crossing in cross walk - not at intersection' then 'Crossing In Cross Walk - Not At Intersection'
    when lower(trim({{ column_name }})) = 'approaching/leaving school bus' then 'Approaching/Leaving School Bus'
    else 'Unknown'
end
{% endmacro %}