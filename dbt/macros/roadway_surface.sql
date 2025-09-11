{% macro roadway_surface(column_name) %}
CASE
    WHEN {{ column_name }} = 'A' THEN 'Dry'
    WHEN {{ column_name }} = 'B' THEN 'Wet'
    WHEN {{ column_name }} = 'C' THEN 'Snowy/Icy'
    WHEN {{ column_name }} = 'D' THEN 'Slippery (Mud, Oil, etc)'
    WHEN {{ column_name }} IS NULL THEN 'Not Stated'
    ELSE INITCAP(LOWER({{ column_name }}))
END
{% endmacro %}
