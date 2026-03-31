-- int_underlying_bars_daily
-- Unified daily bar panel from ALL sources in canonical parquet.
-- Deduplicates by symbol + date, preferring: yahoo > stooq > alphaquant_cache > other.
-- Downstream marts and screens join from this model.

with all_sources as (
    select *,
        case source
            when 'yahoo'            then 1
            when 'stooq'            then 2
            when 'alphaquant_cache' then 3
            else                         4
        end as src_priority
    from read_parquet(
        '{{ var("market_lake_root") }}/canonical/facts/fact_underlying_bar_daily/**/*.parquet',
        union_by_name = true
    )
    where close is not null
      and date  is not null
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by symbol, date
                order by src_priority asc
            ) as rn
        from all_sources
    )
    where rn = 1
)

select
    try_cast(symbol_id as bigint)   as symbol_id,
    upper(trim(symbol))             as symbol,
    try_cast(date as date)          as date,
    try_cast(open as double)        as open,
    try_cast(high as double)        as high,
    try_cast(low as double)         as low,
    try_cast(close as double)       as close,
    try_cast(adj_close as double)   as adj_close,
    try_cast(volume as bigint)      as volume,
    source,
    year
from deduped
