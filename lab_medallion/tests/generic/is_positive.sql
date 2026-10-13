{% test is_positive(model, column_name) %}

-- Returns the rows whose value is zero or negative
select {{ column_name }}
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}
