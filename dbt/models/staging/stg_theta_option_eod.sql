-- stg_theta_option_eod
-- Reads canonical option EOD data.
-- Placeholder parquet ensures this always resolves even before real data is ingested.

select contract_id,
    try_cast(symbol_id as bigint)           as symbol_id,
    upper(trim(underlying_symbol))          as underlying_symbol,
    try_cast(date as date)                  as date,
    try_cast(bid as double)                 as bid,
    try_cast(ask as double)                 as ask,
    try_cast(last as double)                as last,
    try_cast(volume as bigint)              as volume,
    try_cast(open_interest as bigint)       as open_interest,
    try_cast(iv as double)                  as iv,
    try_cast(delta as double)               as delta,
    try_cast(gamma as double)               as gamma,
    try_cast(theta as double)               as theta,
    try_cast(vega as double)                as vega,
    source, year, month
from read_parquet(
    '{{ var("market_lake_root") }}/canonical/facts/fact_option_eod/**/*.parquet',
    union_by_name=true
)
where contract_id is not null
  and date is not null
