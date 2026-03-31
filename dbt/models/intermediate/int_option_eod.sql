with eod as (select * from {{ ref('stg_theta_option_eod') }}),
     c   as (select contract_id, expiry, strike, option_type, multiplier from {{ ref('stg_theta_contracts') }})
select e.contract_id, e.symbol_id, e.underlying_symbol, e.date,
    c.expiry, c.strike, c.option_type, c.multiplier,
    datediff('day', e.date, c.expiry)          as dte,
    e.bid, e.ask, (e.bid + e.ask) / 2.0        as mid,
    e.last, e.volume, e.open_interest,
    e.iv, e.delta, e.gamma, e.theta, e.vega,
    e.source, e.year, e.month
from eod e left join c using (contract_id)
where e.date <= c.expiry
