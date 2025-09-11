{% macro boolean_to_yesno(column_name) %}
    case
        when {{ column_name }} = true then 'Yes'
        when {{ column_name }} = false then 'No'
        else 'Unknown'
    end
{% endmacro %}