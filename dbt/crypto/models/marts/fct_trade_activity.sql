-- Fact: trade activity at symbol + hour grain, aggregated from the raw stream.
-- Demonstrates aggregating high-volume streaming events into an analytics-ready fact.

with hourly as (

    select
        symbol,
        date_trunc('hour', event_time)                              as trade_hour,
        count(*)                                                    as trade_count,
        sum(quantity)                                               as total_quantity,
        sum(notional_value)                                         as total_notional,
        sum(case when side = 'buy' then 1 else 0 end)               as buy_count,
        sum(case when side = 'sell' then 1 else 0 end)              as sell_count,
        avg(price)                                                  as avg_price,
        min(price)                                                  as min_price,
        max(price)                                                  as max_price
    from {{ ref('stg_trades') }}
    group by 1, 2

)

select
    d.symbol_key,
    h.symbol,
    h.trade_hour,
    h.trade_count,
    h.total_quantity,
    h.total_notional,
    h.buy_count,
    h.sell_count,
    h.avg_price,
    h.min_price,
    h.max_price
from hourly h
inner join {{ ref('dim_symbol') }} d
    on h.symbol = d.symbol
