-- int_macro_series
-- All macro observations forward-filled to the daily equity date spine.
-- Sources: FRED (rates, VIX, spreads, inflation), CBOE vol indices, FF factors.
-- One row per series per date — null-safe forward fill across calendar gaps.

with macro as (
    select * from {{ ref('stg_fred_macro') }}
),

ff as (
    -- Reshape FF factors from wide → long so they join the same macro surface
    select date, 'FF_MKT_RF' as series_id, 'FF Market Excess Return' as label, mkt_rf as value, source from {{ ref('stg_ff_factors') }} where mkt_rf is not null
    union all
    select date, 'FF_SMB',   'FF Small Minus Big',                     smb,    source from {{ ref('stg_ff_factors') }} where smb    is not null
    union all
    select date, 'FF_HML',   'FF High Minus Low (Value)',               hml,    source from {{ ref('stg_ff_factors') }} where hml    is not null
    union all
    select date, 'FF_RMW',   'FF Robust Minus Weak (Profitability)',    rmw,    source from {{ ref('stg_ff_factors') }} where rmw    is not null
    union all
    select date, 'FF_CMA',   'FF Conservative Minus Aggressive',        cma,    source from {{ ref('stg_ff_factors') }} where cma    is not null
    union all
    select date, 'FF_RF',    'FF Risk-Free Rate (Daily)',                rf,     source from {{ ref('stg_ff_factors') }} where rf     is not null
    union all
    select date, 'FF_MOM',   'FF Momentum Factor',                      mom,    source from {{ ref('stg_ff_factors') }} where mom    is not null
),

all_series as (
    select series_id, label, date, value, source from macro
    union all
    select series_id, label, date, value, source from ff
),

-- Date spine from equity data
dates as (
    select distinct date from {{ ref('int_underlying_bars_daily') }}
),

-- Cross all series × all dates
grid as (
    select
        s.series_id,
        s.label,
        s.source,
        d.date
    from (select distinct series_id, label, source from all_series) s
    cross join dates d
),

joined as (
    select
        g.series_id,
        g.label,
        g.source,
        g.date,
        o.value
    from grid g
    left join all_series o
        on  o.series_id = g.series_id
        and o.date      = g.date
),

filled as (
    select
        series_id,
        label,
        source,
        date,
        last_value(value ignore nulls) over (
            partition by series_id
            order by date
            rows between unbounded preceding and current row
        ) as value
    from joined
)

select * from filled
where value is not null
