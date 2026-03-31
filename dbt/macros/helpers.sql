{% macro market_lake_root() %}{{ var('market_lake_root', env_var('MARKET_LAKE_ROOT', '.')) }}{% endmacro %}
{% macro parquet_glob(rel) %}'{{ market_lake_root() }}/{{ rel }}'{% endmacro %}
