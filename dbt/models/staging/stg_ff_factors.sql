-- stg_ff_factors
-- Kenneth French Fama-French factors (FF5 + momentum), daily.
-- Factors are in decimal form (0.01 = 1%).
-- mkt_rf: market excess return
-- smb: small minus big
-- hml: high minus low (value)
-- rmw: robust minus weak (profitability)
-- cma: conservative minus aggressive (investment)
-- rf: risk-free rate
-- mom: momentum factor

select
    try_cast(date as date)          as date,
    try_cast(mkt_rf as double)      as mkt_rf,
    try_cast(smb    as double)      as smb,
    try_cast(hml    as double)      as hml,
    try_cast(rmw    as double)      as rmw,
    try_cast(cma    as double)      as cma,
    try_cast(rf     as double)      as rf,
    try_cast(mom    as double)      as mom,
    source,
    year,
    month
from read_parquet(
    '{{ var("market_lake_root") }}/canonical/facts/fact_ff_factors_daily/**/*.parquet',
    union_by_name = true
)
where date is not null
