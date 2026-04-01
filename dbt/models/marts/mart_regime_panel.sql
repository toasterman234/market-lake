-- mart_regime_panel
-- Step 2: Add static labels and dedup on top of mart_regime_panel_base.
-- Split into two models to avoid DuckDB INTERNAL Error caused by
-- LAG() + ROW_NUMBER() window functions in the same query plan.
-- The base model handles the LAG window; this model handles dedup + labels.

select
    date,
    spy_close,
    spy_return_1d,
    spy_return_21d,
    spy_rv20,
    spy_mom_12m,
    fed_funds_rate,
    rate_10y,
    rate_2y,
    yield_curve_10y2y,
    yield_curve_10y3m,
    unemployment_rate,
    cpi,
    vix,
    vix3m,
    gold_vix,
    oil_vix,
    hy_spread,
    usd_index,
    vix_ts_slope,
    spy_iv30,
    spy_ivr,
    spy_vrp,
    ff_mkt_rf,
    ff_smb,
    ff_hml,
    ff_rmw,
    ff_cma,
    ff_rf,
    ff_mom,
    vvix,
    skew_index,
    m2_money_supply,
    wti_oil,
    epu_index,
    kc_stress_index,
    trend_regime,
    -- Vol regime label (static computation — no window needed)
    case
        when vix >= 30 then 'high_vol'
        when vix >= 20 then 'elevated_vol'
        when vix <  15 then 'low_vol'
        else                'normal_vol'
    end                                                 as vol_regime,
    -- Yield curve regime
    case
        when yield_curve_10y2y <  0   then 'inverted'
        when yield_curve_10y2y >= 1   then 'steep'
        else                               'flat'
    end                                                 as yield_curve_regime,
    -- VIX term structure
    case
        when vix > 0 and vix3m > 0 and vix3m < vix
                                      then 'backwardation'
        else                               'contango'
    end                                                 as vix_term_structure

from {{ ref('mart_regime_panel_base') }}

qualify row_number() over (partition by date order by spy_close) = 1
