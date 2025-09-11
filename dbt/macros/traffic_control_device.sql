{% macro traffic_control_device(column_name) %}
case
    when {{ column_name }} = 'A' then 'Controls Functioning'
    when {{ column_name }} = 'B' then 'Controls Not Functioning'
    when {{ column_name }} = 'C' then 'Controls Obscured'
    when {{ column_name }} = 'D' then 'No Controls Present/Factor'
    when {{ column_name }} is null then 'Not Stated'
    else initcap(lower({{ column_name }}))
end
{% endmacro %}
