-- stg_theta_contracts
-- Reads canonical option contract dimension.
-- Placeholder parquet ensures this always resolves even before real data is ingested.

select contract_id,
    try_cast(symbol_id as bigint)           as symbol_id,
    upper(trim(underlying_symbol))          as underlying_symbol,
    occ_symbol,
    try_cast(expiry as date)                as expiry,
    try_cast(strike as double)              as strike,
    upper(trim(option_type))                as option_type,
    try_cast(multiplier as integer)         as multiplier,
    try_cast(first_seen as date)            as first_seen,
    try_cast(last_seen as date)             as last_seen
from read_parquet(
    '{{ var("market_lake_root") }}/canonical/dimensions/dim_option_contract/**/*.parquet',
    union_by_name=true
)
where contract_id is not null
