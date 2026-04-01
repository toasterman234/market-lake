-- mart_optimization_inputs
-- Optimizer-ready panel. One row per symbol per date.
-- Returns, vol, momentum, VRP signals, and regime/factor context.

with equity as (
    select
        symbol_id, symbol, date, adj_close,
        return_1d, log_return_1d, return_21d,
        realized_vol_20d, mom_1m, mom_12m, year
    from {{ ref('mart_backtest_equity_panel') }}
),

vrp as (
    select symbol, date, iv_30d, ivr_252d, vrp_30d
    from {{ ref('stg_theta_vrp_features') }}
),

regime as (
    select
        date, vol_regime, trend_regime, vix, vix3m,
        fed_funds_rate, rate_10y, yield_curve_10y2y, hy_spread,
        ff_mkt_rf, ff_smb, ff_hml, ff_rmw, ff_cma, ff_mom, ff_rf,
        vix_term_structure
    from {{ ref('mart_regime_panel') }}
)

select
    e.symbol_id,
    e.symbol,
    e.date,
    e.adj_close,
    e.return_1d,
    e.log_return_1d,
    e.return_21d,
    e.realized_vol_20d,
    e.mom_1m,
    e.mom_12m,

    -- Annualised estimates
    e.return_21d * (252.0 / 21)                         as ann_return_est,
    e.realized_vol_20d                                   as ann_vol_est,

    -- Excess return (above risk-free)
    e.return_1d - coalesce(r.ff_rf, 0)                  as excess_return_1d,

    -- Sharpe-like signal
    case when e.realized_vol_20d > 0
         then (e.return_21d * (252.0 / 21)) / e.realized_vol_20d
    end                                                  as sharpe_like_signal,

    -- VRP signals
    v.iv_30d,
    v.ivr_252d,
    v.vrp_30d,

    -- Regime context
    r.vol_regime,
    r.trend_regime,
    r.vix,
    r.vix3m,
    r.vix_term_structure,
    r.fed_funds_rate,
    r.rate_10y,
    r.yield_curve_10y2y,
    r.hy_spread,

    -- Fama-French factors (for factor exposure analysis)
    r.ff_mkt_rf,
    r.ff_smb,
    r.ff_hml,
    r.ff_rmw,
    r.ff_cma,
    r.ff_mom,

    e.year

from equity e
left join vrp    v on v.symbol = e.symbol and v.date = e.date
left join regime r on r.date = e.date
