{% macro standardize_road_condition(column_name) %}
case
    when upper(trim({{ column_name }})) like 'HOLES%' then 'Holes, Deep Ruts'
    when upper(trim({{ column_name }})) like 'LOOSE MATERIAL ON ROADWAY%' then 'Loose Material on Roadway'
    when upper(trim({{ column_name }})) like 'OBSTRUCTION%' then 'Obstruction on Roadway'
    when upper(trim({{ column_name }})) like 'CONSTRUCTION%' then 'Construction or Repair Zone'
    when upper(trim({{ column_name }})) like 'REDUCED ROADWAY WIDTH%' then 'Reduced Roadway Width'
    when upper(trim({{ column_name }})) like 'FLOODED%' then 'Flooded'
    when upper(trim({{ column_name }})) like 'OTHER%' then 'Other'
    when upper(trim({{ column_name }})) like 'NO UNUSUAL CONDITION%' then 'No Unusual Conditions'
    ELSE 'Unknown'
end
{% endmacro %}
