-- mart_regime_panel
-- Daily regime characterization for SPY with macro and factor context.
-- Sources: SPY price (equity panel), FRED, CBOE vol, Fama-French factors.

with e as (
    select symbol, date, adj_close, return_1d, return_21d, realized_vol_20d, mom_12m
    from {{ ref('mart_backtest_equity_panel') }}
    where symbol = 'SPY'
),

macro as (
    select
        date,
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
        -- FF factors
        max(case when series_id = 'FF_MKT_RF'    then value end) as ff_mkt_rf,
        max(case when series_id = 'FF_SMB'        then value end) as ff_smb,
        max(case when series_id = 'FF_HML'        then value end) as ff_hml,
        max(case when series_id = 'FF_RMW'        then value end) as ff_rmw,
        max(case when series_id = 'FF_CMA'        then value end) as ff_cma,
        max(case when series_id = 'FF_RF'         then value end) as ff_rf,
        max(case when series_id = 'FF_MOM'        then value end) as ff_mom,
        -- New series added Mar 2026
        max(case when series_id = 'VVIX'          then value end) as vvix,
        max(case when series_id = 'SKEW'          then value end) as skew_index,
        max(case when series_id = 'M2SL'          then value end) as m2_money_supply,
        max(case when series_id = 'DCOILWTICO'    then value end) as wti_oil,
        max(case when series_id = 'USEPUINDXD'    then value end) as epu_index,
        max(case when series_id = 'KCFSI'         then value end) as kc_stress_index
    from {{ ref('int_macro_series') }}
    group by date
),

vrp as (
    select symbol, date, iv_30d, ivr_252d, vrp_30d
    from {{ ref('stg_theta_vrp_features') }}
    where symbol = 'SPY'
),

joined as (
    select
        e.date,
        e.adj_close                                         as spy_close,
        e.return_1d                                         as spy_return_1d,
        e.return_21d                                        as spy_return_21d,
        e.realized_vol_20d                                  as spy_rv20,
        e.mom_12m                                           as spy_mom_12m,

        -- Rates
        m.fed_funds_rate,
        m.rate_10y,
        m.rate_2y,
        m.yield_curve_10y2y,
        m.yield_curve_10y3m,
        m.unemployment_rate,
        m.cpi,

        -- Vol surface
        m.vix,
        m.vix3m,
        m.gold_vix,
        m.oil_vix,
        m.hy_spread,
        m.usd_index,

        -- VIX term structure slope (higher = contango, lower/negative = backwardation)
        case when m.vix > 0 and m.vix3m > 0
             then m.vix3m / m.vix - 1 end                  as vix_ts_slope,

        -- SPY VRP / IV
        v.iv_30d                                            as spy_iv30,
        v.ivr_252d                                          as spy_ivr,
        v.vrp_30d                                           as spy_vrp,

        -- Fama-French factors
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

        -- Regime labels
        case
            when m.vix >= 30 then 'high_vol'
            when m.vix >= 20 then 'elevated_vol'
            when m.vix <  15 then 'low_vol'
            else                   'normal_vol'
        end                                                 as vol_regime,

        case
            when e.adj_close < lag(e.adj_close, 63) over (order by e.date) * 0.80
                                                            then 'bear'
            when e.adj_close > lag(e.adj_close, 63) over (order by e.date) * 1.05
                                                            then 'bull'
            else                                                'neutral'
        end                                                 as trend_regime,

        case
            when m.yield_curve_10y2y <  0   then 'inverted'
            when m.yield_curve_10y2y >= 1   then 'steep'
            else                                  'flat'
        end                                                 as yield_curve_regime,

        case
            when m.vix > 0 and m.vix3m > 0 and m.vix3m < m.vix
                                                            then 'backwardation'
            else                                                'contango'
        end                                                 as vix_term_structure

    from e
    left join macro m using (date)
    left join vrp   v using (date)
)

select * from joined
