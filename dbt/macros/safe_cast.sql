{% macro safe_cast(expr, dtype) -%}
try_cast({{ expr }} as {{ dtype }})
{%- endmacro %}
