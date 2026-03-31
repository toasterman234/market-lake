with e as (select symbol, date, adj_close, return_1d, return_21d, realized_vol_20d, mom_12m
           from {{ ref('mart_backtest_equity_panel') }} where symbol='SPY'),
     m as (select date,
               max(case when series_id='FEDFUNDS' then value end) as fed_funds_rate,
               max(case when series_id='DGS10'    then value end) as rate_10y,
               max(case when series_id='DGS2'     then value end) as rate_2y,
               max(case when series_id='T10Y2Y'   then value end) as yield_curve_10y2y,
               max(case when series_id='UNRATE'   then value end) as unemployment_rate,
               max(case when series_id='VIXCLS'   then value end) as vix
           from {{ ref('int_macro_series') }} group by date),
     v as (select symbol, date, iv_30d, ivr_252d, vrp_30d
           from {{ ref('stg_theta_vrp_features') }} where symbol='SPY')
select e.date, e.adj_close as spy_close, e.return_1d as spy_return_1d,
    e.realized_vol_20d as spy_rv20, e.mom_12m as spy_mom_12m,
    m.fed_funds_rate, m.rate_10y, m.yield_curve_10y2y, m.unemployment_rate, m.vix,
    v.iv_30d as spy_iv30, v.ivr_252d as spy_ivr, v.vrp_30d as spy_vrp,
    case when m.vix >= 30 then 'high_vol' when m.vix >= 20 then 'elevated_vol'
         when m.vix < 15 then 'low_vol' else 'normal_vol' end as vol_regime,
    case when e.adj_close < lag(e.adj_close,63) over (order by e.date) * 0.80 then 'bear'
         when e.adj_close > lag(e.adj_close,63) over (order by e.date) * 1.05 then 'bull'
         else 'neutral' end as trend_regime,
    case when m.yield_curve_10y2y < 0 then 'inverted'
         when m.yield_curve_10y2y >= 1 then 'steep' else 'flat' end as yield_curve_regime
from e left join m using (date) left join v using (date)
