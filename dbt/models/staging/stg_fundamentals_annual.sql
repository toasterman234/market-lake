-- stg_fundamentals_annual
-- Annual financial ratios computed from yfinance statements.
-- ETFs excluded. Covers ~500 equity symbols, 4 years back.

select
    try_cast(symbol_id as bigint)           as symbol_id,
    upper(trim(symbol))                     as symbol,
    try_cast(fiscal_year_end as date)       as fiscal_year_end,
    period_type,
    try_cast(gross_margin       as double)  as gross_margin,
    try_cast(ebit_margin        as double)  as ebit_margin,
    try_cast(net_margin         as double)  as net_margin,
    try_cast(roe                as double)  as roe,
    try_cast(roa                as double)  as roa,
    try_cast(current_ratio      as double)  as current_ratio,
    try_cast(debt_to_equity     as double)  as debt_to_equity,
    try_cast(debt_to_assets     as double)  as debt_to_assets,
    try_cast(interest_coverage  as double)  as interest_coverage,
    try_cast(fcf_margin         as double)  as fcf_margin,
    try_cast(earnings_quality   as double)  as earnings_quality,
    try_cast(revenue_growth_yoy as double)  as revenue_growth_yoy,
    try_cast(earnings_growth_yoy as double) as earnings_growth_yoy,
    try_cast(altman_z_score     as double)  as altman_z_score,
    -- piotroski_score stored as mixed type — normalize to integer
    try_cast(try_cast(piotroski_score as double) as integer)  as piotroski_score,
    year
from read_parquet(
    '{{ var("market_lake_root") }}/canonical/facts/fact_fundamentals_annual/**/*.parquet',
    union_by_name = true
)
where symbol is not null
  and fiscal_year_end is not null
