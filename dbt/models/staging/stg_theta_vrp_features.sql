-- Deduplicate on (symbol, date) — multiple ingest runs can create exact duplicate rows.
with raw as (
select try_cast(symbol_id as bigint) as symbol_id, upper(trim(symbol)) as symbol,
    try_cast(date as date) as date, try_cast(spot_price as double) as spot_price,
    -- Cap IV at 3.0 (300%) — values above this are ThetaData computation artifacts
    -- for very illiquid/far-OTM options, not real market implied volatilities
    LEAST(try_cast(iv_30d as double), 3.0)         as iv_30d,
    LEAST(try_cast(iv_60d as double), 3.0)         as iv_60d,
    try_cast(hv20 as double) as hv20, try_cast(hv30 as double) as hv30,
    try_cast(vrp_30d as double) as vrp_30d, try_cast(vrp_60d as double) as vrp_60d,
    try_cast(ivr_252d as double) as ivr_252d, try_cast(ivp_252d as double) as ivp_252d,
    try_cast(ts_slope_30_60 as double) as ts_slope_30_60,
    try_cast(put_skew_25d as double) as put_skew_25d,
    try_cast(put_skew_10d as double) as put_skew_10d,
    try_cast(pc_volume_ratio as double) as pc_volume_ratio,
    source, year, month
from read_parquet('{{ var('market_lake_root') }}/canonical/features/fact_option_feature_daily/**/*.parquet', union_by_name=true)
where symbol is not null and date is not null
),

deduped as (
    select *,
        row_number() over (partition by symbol, date order by symbol) as rn
    from raw
)

select * exclude (rn) from deduped where rn = 1
