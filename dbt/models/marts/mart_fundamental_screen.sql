-- mart_fundamental_screen
-- Composite premium-selling scanner: VRP signals + fundamental quality.
-- Reads fundamentals directly from parquet with explicit casts to avoid
-- type conflicts from mixed BIGINT/DOUBLE piotroski_score across parquet files.

with raw_fundamentals as (
    select
        upper(trim(symbol))                                      as symbol,
        try_cast(fiscal_year_end as date)                        as fiscal_year_end,
        try_cast(gross_margin       as double)                   as gross_margin,
        try_cast(net_margin         as double)                   as net_margin,
        try_cast(roe                as double)                   as roe,
        try_cast(roa                as double)                   as roa,
        try_cast(current_ratio      as double)                   as current_ratio,
        try_cast(debt_to_equity     as double)                   as debt_to_equity,
        try_cast(interest_coverage  as double)                   as interest_coverage,
        try_cast(fcf_margin         as double)                   as fcf_margin,
        try_cast(earnings_quality   as double)                   as earnings_quality,
        try_cast(revenue_growth_yoy as double)                   as revenue_growth_yoy,
        try_cast(altman_z_score     as double)                   as altman_z_score,
        -- Normalize piotroski_score to INTEGER regardless of parquet type
        try_cast(try_cast(piotroski_score as double) as integer) as piotroski_score
    from read_parquet(
        '{{ var("market_lake_root") }}/canonical/facts/fact_fundamentals_annual/**/*.parquet',
        union_by_name = true
    )
    where symbol is not null and fiscal_year_end is not null
),

latest_fundamentals as (
    select *,
        row_number() over (partition by symbol order by fiscal_year_end desc) as rn
    from raw_fundamentals
),

fundamentals as (
    select * from latest_fundamentals where rn = 1
),

screening as (
    -- Take the most recent row per symbol rather than filtering to a single global max date.
    -- Different symbols have different data lags (chains updated on different days),
    -- so a global max date filter would silently drop symbols updated 1-2 days earlier.
    select symbol, date, ivr_252d, ivp_252d, vrp_30d,
           ivr_rank, vrp_rank, mom_rank, iv_30d, realized_vol_20d, mom_12m
    from {{ ref('mart_screening_panel') }}
    qualify row_number() over (partition by symbol order by date desc) = 1
),

sym as (
    select symbol, sector, industry
    from read_parquet(
        '{{ var("market_lake_root") }}/canonical/dimensions/dim_symbol/**/*.parquet',
        union_by_name = true
    )
    where asset_type = 'stock'
)

select
    s.symbol,
    s.date,
    sym.sector,
    sym.industry,

    -- VRP signals
    s.ivr_252d,
    s.ivp_252d,
    s.vrp_30d,
    s.iv_30d,
    s.realized_vol_20d,
    s.mom_12m,
    s.ivr_rank,
    s.vrp_rank,
    s.mom_rank,

    -- Fundamentals (most recent fiscal year)
    f.fiscal_year_end,
    f.gross_margin,
    f.net_margin,
    f.roe,
    f.roa,
    f.current_ratio,
    f.debt_to_equity,
    f.interest_coverage,
    f.fcf_margin,
    f.earnings_quality,
    f.revenue_growth_yoy,
    f.altman_z_score,
    f.piotroski_score,

    -- Hard filter flags
    (f.piotroski_score >= 5)                                       as is_financially_healthy,
    (f.altman_z_score > 2.99  or f.altman_z_score is null)        as is_not_distressed,
    (f.debt_to_equity < 2.0   or f.debt_to_equity  is null)       as has_manageable_debt,
    (f.current_ratio  > 1.0   or f.current_ratio   is null)       as has_adequate_liquidity,

    -- Position sizing tier
    case
        when f.piotroski_score >= 7
             and (f.altman_z_score > 2.99 or f.altman_z_score is null)
             and s.ivr_252d >= 0.5          then 'FULL SIZE'
        when f.piotroski_score >= 5
             and (f.altman_z_score > 1.81 or f.altman_z_score is null)
             and s.ivr_252d >= 0.4          then 'HALF SIZE'
        when f.piotroski_score >= 3
             and s.ivr_252d >= 0.3          then 'QUARTER SIZE'
        else                                     'SKIP'
    end                                                            as fundamental_tier,

    -- Composite score: IVR rank 60% + Piotroski 40%
    round(
        coalesce(s.ivr_rank, 0) * 0.6 +
        coalesce(f.piotroski_score::double / 9.0, 0) * 0.4,
        4
    )                                                              as composite_score

from screening s
left join fundamentals f using (symbol)
left join sym           using (symbol)
