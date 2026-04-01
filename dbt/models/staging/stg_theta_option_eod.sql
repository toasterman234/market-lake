-- stg_theta_option_eod
-- Reads canonical option EOD data.
-- Passes through expiry/strike/option_type if present in the raw files
-- so int_option_eod can use them without requiring the contract dimension.

-- Deduplicate on (contract_id, date) — chain parquet quarterly boundaries create
-- overlapping rows. Take row with highest bid (most liquid/recent snapshot).
with raw as (
select
    contract_id,
    try_cast(symbol_id as bigint)               as symbol_id,
    -- 50.6M rows from thetadata_vrp_validate source have null underlying_symbol.
    -- contract_id format is SYMBOL|EXPIRY|STRIKE|TYPE — parse symbol as fallback.
    coalesce(
        nullif(upper(trim(underlying_symbol)), 'NAN'),
        split_part(contract_id, '|', 1)
    )                                                   as underlying_symbol,
    try_cast(date as date)                      as date,

    -- expiry/strike/option_type may come directly from chain parquets
    try_cast(expiry as date)                    as expiry,
    try_cast(strike as double)                  as strike,
    upper(trim(option_type))                    as option_type,

    try_cast(bid as double)                     as bid,
    try_cast(ask as double)                     as ask,
    try_cast(mid as double)                     as mid,
    try_cast(last as double)                    as last,
    try_cast(volume as bigint)                  as volume,
    try_cast(open_interest as bigint)           as open_interest,
    try_cast(iv as double)                      as iv,
    try_cast(delta as double)                   as delta,
    try_cast(gamma as double)                   as gamma,
    try_cast(theta as double)                   as theta,
    try_cast(vega as double)                    as vega,
    source,
    year,
    month

from read_parquet(
    '{{ var("market_lake_root") }}/canonical/facts/fact_option_eod/**/*.parquet',
    union_by_name = true
)
where contract_id is not null
  and date is not null
),

deduped as (
    select *,
        row_number() over (
            partition by contract_id, date
            order by coalesce(bid, -1) desc
        ) as rn
    from raw
)

select * exclude (rn) from deduped where rn = 1
