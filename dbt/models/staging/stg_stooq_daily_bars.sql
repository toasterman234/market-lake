with source as (
    select * from read_parquet('{{ var('market_lake_root') }}/canonical/facts/fact_underlying_bar_daily/**/*.parquet', union_by_name=true)
    where source = 'stooq'
)
select
    try_cast(symbol_id as bigint) as symbol_id, upper(trim(symbol)) as symbol,
    try_cast(date as date) as date,
    try_cast(open as double) as open, try_cast(high as double) as high,
    try_cast(low as double) as low,  try_cast(close as double) as close,
    try_cast(adj_close as double) as adj_close, try_cast(volume as bigint) as volume,
    source, year
from source where close is not null and date is not null
