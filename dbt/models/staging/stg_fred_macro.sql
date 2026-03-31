select upper(trim(series_id)) as series_id, label,
    try_cast(date as date) as date, try_cast(value as double) as value,
    source, year
from read_parquet('{{ var('market_lake_root') }}/canonical/facts/fact_macro_series/**/*.parquet', union_by_name=true)
where series_id is not null and date is not null
