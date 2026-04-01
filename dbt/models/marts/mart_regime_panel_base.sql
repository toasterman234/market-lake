-- mart_regime_panel_base
-- Daily SPY regime + macro signals base table.
--
-- DuckDB 1.5.1 INTERNAL Error workaround:
--   The engine hits "inequal types DATE != VARCHAR" when the full 26-col macro
--   pivot CTE is joined with BOTH mart_backtest_equity_panel (TABLE) AND
--   stg_theta_vrp_features (VIEW) in a single query.
--   Root cause confirmed by bisection: the VIEW triggers lazy type resolution
--   that conflicts with the wide pivot under DuckDB's cross-schema optimizer.
--
--   Fix: read VRP directly from the canonical parquet files, bypassing the VIEW.
--   This was confirmed working in direct Python/DuckDB testing.

with

-- SPY prices + LAG-based trend in its own isolated CTE
-- (LAG isolated from the multi-schema joins avoids a secondary DuckDB issue)
spy_base as (
    select
        try_cast(date as date) as date,
        adj_close,
        return_1d,
        return_21d,
        realized_vol_20d,
        mom_12m
    from {{ ref('mart_backtest_equity_panel') }}
    where symbol = 'SPY'
),

spy_prices as (
    select
        date,
        adj_close,
        return_1d,
        return_21d,
        realized_vol_20d,
        mom_12m,
        case
            when adj_close < lag(adj_close, 63) over (order by date) * 0.80
                then 'bear'
            when adj_close > lag(adj_close, 63) over (order by date) * 1.05
                then 'bull'
            else 'neutral'
        end as trend_regime
    from spy_base
),

macro as (
    select
        try_cast(date as date) as date,
        max(case when series_id = 'FEDFUNDS'      then value end) as fed_funds_rate,
        max(case when series_id = 'DGS10'         then value end) as rate_10y,
        max(case when series_id = 'DGS2'          then value end) as rate_2y,
        max(case when series_id = 'T10Y2Y'        then value end) as yield_curve_10y2y,
        max(case when series_id = 'T10Y3M'        then value end) as yield_curve_10y3m,
        max(case when series_id = 'UNRATE'        then value end) as unemployment_rate,
        max(case when series_id = 'CPIAUCSL'      then value end) as cpi,
        max(case when series_id = 'VIXCLS'        then value end) as vix,
        max(case when series_id = 'VIX3M'         then value end) as vix3m,
        max(case when series_id = 'GVZ'           then value end) as gold_vix,
        max(case when series_id = 'OVX'           then value end) as oil_vix,
        max(case when series_id = 'BAMLH0A0HYM2' then value end) as hy_spread,
        max(case when series_id = 'DTWEXBGS'      then value end) as usd_index,
        max(case when series_id = 'FF_MKT_RF'    then value end) as ff_mkt_rf,
        max(case when series_id = 'FF_SMB'        then value end) as ff_smb,
        max(case when series_id = 'FF_HML'        then value end) as ff_hml,
        max(case when series_id = 'FF_RMW'        then value end) as ff_rmw,
        max(case when series_id = 'FF_CMA'        then value end) as ff_cma,
        max(case when series_id = 'FF_RF'         then value end) as ff_rf,
        max(case when series_id = 'FF_MOM'        then value end) as ff_mom,
        max(case when series_id = 'VVIX'          then value end) as vvix,
        max(case when series_id = 'SKEW'          then value end) as skew_index,
        max(case when series_id = 'M2SL'          then value end) as m2_money_supply,
        max(case when series_id = 'DCOILWTICO'    then value end) as wti_oil,
        max(case when series_id = 'USEPUINDXD'    then value end) as epu_index,
        max(case when series_id = 'KCFSI'         then value end) as kc_stress_index
    from {{ ref('int_macro_series') }}
    group by 1
),

-- Read VRP directly from parquet — bypasses stg_theta_vrp_features VIEW which
-- triggers DuckDB INTERNAL Error when joined alongside the wide macro pivot.
vrp as (
    select
        try_cast(date as date) as date,
        iv_30d,
        ivr_252d,
        vrp_30d
    from read_parquet(
        '{{ var("market_lake_root") }}/canonical/features/fact_option_feature_daily/**/*.parquet',
        union_by_name = true
    )
    where symbol = 'SPY'
)

select
    p.date,
    p.adj_close                                         as spy_close,
    p.return_1d                                         as spy_return_1d,
    p.return_21d                                        as spy_return_21d,
    p.realized_vol_20d                                  as spy_rv20,
    p.mom_12m                                           as spy_mom_12m,
    m.fed_funds_rate,
    m.rate_10y,
    m.rate_2y,
    m.yield_curve_10y2y,
    m.yield_curve_10y3m,
    m.unemployment_rate,
    m.cpi,
    m.vix,
    m.vix3m,
    m.gold_vix,
    m.oil_vix,
    m.hy_spread,
    m.usd_index,
    case when m.vix > 0 and m.vix3m > 0
         then m.vix3m / m.vix - 1 end                  as vix_ts_slope,
    v.iv_30d                                            as spy_iv30,
    v.ivr_252d                                          as spy_ivr,
    v.vrp_30d                                           as spy_vrp,
    m.ff_mkt_rf,
    m.ff_smb,
    m.ff_hml,
    m.ff_rmw,
    m.ff_cma,
    m.ff_rf,
    m.ff_mom,
    m.vvix,
    m.skew_index,
    m.m2_money_supply,
    m.wti_oil,
    m.epu_index,
    m.kc_stress_index,
    p.trend_regime
from spy_prices p
left join macro m on m.date = p.date
left join vrp   v on v.date = p.date
