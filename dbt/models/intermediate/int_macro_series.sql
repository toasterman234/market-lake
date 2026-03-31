with macro as (select * from {{ ref('stg_fred_macro') }}),
     dates as (select distinct date from {{ ref('int_underlying_bars_daily') }}),
     grid  as (select s.series_id, s.label, d.date
               from (select distinct series_id, label from macro) s cross join dates d),
     joined as (select g.series_id, g.label, g.date, m.value
                from grid g left join macro m on m.series_id=g.series_id and m.date=g.date),
     filled as (select series_id, label, date,
                    last_value(value ignore nulls) over (
                        partition by series_id order by date
                        rows between unbounded preceding and current row
                    ) as value
                from joined)
select * from filled where value is not null
