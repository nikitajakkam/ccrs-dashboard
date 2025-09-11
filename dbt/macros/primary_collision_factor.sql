{% macro primary_collision_factor(column_name) %}
case
    when {{ column_name }} = 'A' then '(Vehicle) Code Violation'
    when {{ column_name }} = 'B' then 'Other Improper Driving'
    when {{ column_name }} = 'C' then 'Other Than Driver'
    when {{ column_name }} = 'D' then 'Unknown'
    when {{ column_name }} = 'E' then 'Fell Asleep'
    when {{ column_name }} is null then 'Not Stated'
    else initcap(lower({{ column_name }}))
end
{% endmacro %}
