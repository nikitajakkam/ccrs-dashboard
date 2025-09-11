{% macro is_preliminary(column_name) %}
    case
        when {{ column_name }} = true then 'Preliminary'
        when {{ column_name }} = false then 'Final'
        else 'Unknown'
    end
{% endmacro %}