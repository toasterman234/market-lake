with e as (select symbol_id, symbol, date, adj_close, return_1d, log_return_1d,
               return_21d, realized_vol_20d, mom_1m, mom_12m, year
           from {{ ref('mart_backtest_equity_panel') }}),
     v as (select symbol, date, iv_30d, ivr_252d, vrp_30d from {{ ref('stg_theta_vrp_features') }}),
     r as (select date, vol_regime, trend_regime, vix, fed_funds_rate, rate_10y from {{ ref('mart_regime_panel') }})
select e.symbol_id, e.symbol, e.date, e.adj_close, e.return_1d, e.log_return_1d,
    e.return_21d, e.realized_vol_20d, e.mom_1m, e.mom_12m,
    e.return_21d * (252.0/21)  as ann_return_est,
    e.realized_vol_20d          as ann_vol_est,
    case when e.realized_vol_20d > 0 then e.mom_12m / e.realized_vol_20d else null end as sharpe_like_signal,
    v.iv_30d, v.ivr_252d, v.vrp_30d,
    r.vol_regime, r.trend_regime, r.vix, r.fed_funds_rate, r.rate_10y, e.year
from e
left join v on v.symbol=e.symbol and v.date=e.date
left join r on r.date=e.date
