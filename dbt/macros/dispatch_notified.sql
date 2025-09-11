{% macro dispatch_notified(column_name) %}
case
    when {{ column_name }} = 'Yes' then 'Yes'
    when {{ column_name }} = 'No' then 'No'
    when {{ column_name }} = 'NotApplicable' then 'Not Applicable'
    else 'Unknown'
end
{% endmacro %}
