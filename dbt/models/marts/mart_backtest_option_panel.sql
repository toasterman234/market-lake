with o as (select * from {{ ref('int_option_eod') }}),
     u as (select symbol, date, close as underlying_close, realized_vol_20d from {{ ref('mart_backtest_equity_panel') }}),
     v as (select symbol, date, iv_30d, ivr_252d, ivp_252d, vrp_30d, ts_slope_30_60, put_skew_25d, pc_volume_ratio
           from {{ ref('stg_theta_vrp_features') }})
select o.contract_id, o.symbol_id, o.underlying_symbol as symbol, o.date, o.expiry, o.dte,
    o.strike, o.option_type, o.multiplier,
    o.bid, o.ask, o.mid, o.last, o.volume as option_volume, o.open_interest,
    o.iv, o.delta, o.gamma, o.theta, o.vega,
    u.underlying_close, u.realized_vol_20d,
    o.strike / nullif(u.underlying_close, 0)        as strike_to_spot,
    abs(o.delta)                                     as abs_delta,
    o.mid / nullif(u.underlying_close, 0) * 100      as mid_pct_spot,
    v.iv_30d, v.ivr_252d, v.ivp_252d, v.vrp_30d, v.ts_slope_30_60, v.put_skew_25d, v.pc_volume_ratio,
    o.year, o.month
from o
left join u on u.symbol=o.underlying_symbol and u.date=o.date
left join v on v.symbol=o.underlying_symbol and v.date=o.date
