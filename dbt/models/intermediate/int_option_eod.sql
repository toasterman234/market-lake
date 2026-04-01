-- int_option_eod
-- Option EOD enriched with contract metadata.
-- Uses expiry from the EOD row itself when available (nullable-safe).
-- Does NOT filter out rows where the contract dimension join fails.

with eod as (
    select * from {{ ref('stg_theta_option_eod') }}
),

contracts as (
    select
        contract_id,
        expiry          as contract_expiry,
        strike          as contract_strike,
        option_type     as contract_option_type,
        multiplier
    from {{ ref('stg_theta_contracts') }}
),

joined as (
    select
        e.contract_id,
        e.symbol_id,
        e.underlying_symbol,
        e.date,

        -- Use expiry from EOD row first, fall back to contract dim
        coalesce(e.expiry, c.contract_expiry)                           as expiry,

        -- Use strike/type from EOD row first, fall back to contract dim
        coalesce(e.strike, c.contract_strike)                           as strike,
        coalesce(e.option_type, c.contract_option_type)                 as option_type,

        coalesce(c.multiplier, 100)                                     as multiplier,

        -- DTE — computed from whichever expiry we have
        case
            when coalesce(e.expiry, c.contract_expiry) is not null
            then datediff(
                'day',
                e.date,
                coalesce(e.expiry, c.contract_expiry)
            )
        end                                                             as dte,

        e.bid,
        e.ask,
        e.mid,
        e.last,
        e.volume,
        e.open_interest,
        e.iv,
        e.delta,
        e.gamma,
        e.theta,
        e.vega,
        e.source,
        e.year,
        e.month

    from eod e
    left join contracts c using (contract_id)
),

-- Only filter out rows that are clearly past expiry (not rows with unknown expiry)
filtered as (
    select *
    from joined
    where expiry is null          -- unknown expiry: keep the row
       or date <= expiry           -- not yet expired: keep
)

select * from filtered
