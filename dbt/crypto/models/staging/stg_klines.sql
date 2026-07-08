-- Staging: clean + type the daily OHLCV candles. One row per symbol per day.

with source as (

    select * from {{ source('raw', 'klines') }}

)

select
    symbol,
    open_time::date          as trade_date,
    open::numeric            as open_price,
    high::numeric            as high_price,
    low::numeric             as low_price,
    close::numeric           as close_price,
    volume::numeric          as volume,
    (close - open)::numeric  as daily_change,
    case
        when open > 0 then round(((close - open) / open) * 100, 4)
        else 0
    end                      as daily_change_pct
from source
where symbol is not null
  and open > 0
