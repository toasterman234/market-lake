with e as (select symbol_id, symbol, date, close, volume, return_1d, return_21d,
               realized_vol_20d, mom_1m, mom_12m, pct_from_52w_high, pct_from_52w_low
           from {{ ref('mart_backtest_equity_panel') }}),
     v as (select symbol, date, iv_30d, ivr_252d, ivp_252d, vrp_30d, ts_slope_30_60,
               put_skew_25d, pc_volume_ratio, hv30
           from {{ ref('stg_theta_vrp_features') }}),
     r as (select date, vol_regime, trend_regime from {{ ref('mart_regime_panel') }})
select e.symbol_id, e.symbol, e.date, e.close, e.volume, e.return_1d, e.return_21d,
    e.realized_vol_20d, e.mom_1m, e.mom_12m, e.pct_from_52w_high, e.pct_from_52w_low,
    v.iv_30d, v.ivr_252d, v.ivp_252d, v.vrp_30d, v.ts_slope_30_60, v.put_skew_25d, v.pc_volume_ratio, v.hv30,
    percent_rank() over (partition by e.date order by v.ivr_252d nulls last) as ivr_rank,
    percent_rank() over (partition by e.date order by v.vrp_30d nulls last)  as vrp_rank,
    percent_rank() over (partition by e.date order by e.mom_12m nulls last)  as mom_rank,
    r.vol_regime, r.trend_regime
from e
left join v on v.symbol=e.symbol and v.date=e.date
left join r on r.date::DATE=e.date::DATE
