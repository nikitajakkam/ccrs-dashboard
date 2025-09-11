{% macro hit_and_run(column_name) %}
case
    when {{ column_name }} = 'M' then 'Felony'
    when {{ column_name }} = 'F' then 'Misdemeanor'
    else 'None'
end
{% endmacro %}
