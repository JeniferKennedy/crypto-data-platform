-- Fact: daily price grain. One row per symbol per day, joined to the symbol dimension.
-- This is the table analysts and dashboards query most.

select
    d.symbol_key,
    k.symbol,
    k.trade_date,
    k.open_price,
    k.high_price,
    k.low_price,
    k.close_price,
    k.volume,
    k.daily_change,
    k.daily_change_pct
from {{ ref('stg_klines') }} k
inner join {{ ref('dim_symbol') }} d
    on k.symbol = d.symbol
