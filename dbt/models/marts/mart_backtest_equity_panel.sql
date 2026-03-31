-- mart_backtest_equity_panel
-- One row per symbol per date with returns, vol, momentum, and range signals.

with base as (
    select * from {{ ref('int_underlying_bars_daily') }}
),

-- Step 1: compute lag so we can use it in both returns and vol without nesting
with_lag as (
    select
        *,
        lag(adj_close, 1)   over (partition by symbol order by date) as prev_1d,
        lag(adj_close, 5)   over (partition by symbol order by date) as prev_5d,
        lag(adj_close, 21)  over (partition by symbol order by date) as prev_21d,
        lag(adj_close, 63)  over (partition by symbol order by date) as prev_63d,
        lag(adj_close, 252) over (partition by symbol order by date) as prev_252d,
        max(adj_close) over (partition by symbol order by date rows between 251 preceding and current row) as high_52w,
        min(adj_close) over (partition by symbol order by date rows between 251 preceding and current row) as low_52w
    from base
),

-- Step 2: compute log return from pre-calculated lag
with_logret as (
    select
        *,
        ln(adj_close / nullif(prev_1d, 0)) as log_return_1d
    from with_lag
),

-- Step 3: rolling vol using the already-computed log_return_1d (no nesting)
final as (
    select
        symbol_id,
        symbol,
        date,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        source,
        year,

        adj_close / nullif(prev_1d,   0) - 1   as return_1d,
        adj_close / nullif(prev_5d,   0) - 1   as return_5d,
        adj_close / nullif(prev_21d,  0) - 1   as return_21d,
        adj_close / nullif(prev_63d,  0) - 1   as return_63d,

        log_return_1d,

        stddev(log_return_1d) over (
            partition by symbol order by date
            rows between 19 preceding and current row
        ) * sqrt(252)                           as realized_vol_20d,

        adj_close / nullif(prev_252d, 0) - 1   as mom_12m,
        adj_close / nullif(prev_21d,  0) - 1   as mom_1m,

        adj_close / nullif(high_52w, 0) - 1    as pct_from_52w_high,
        adj_close / nullif(low_52w,  0) - 1    as pct_from_52w_low

    from with_logret
)

select * from final
where return_1d is not null
