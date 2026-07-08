-- Custom (singular) data quality test.
-- Business rule: a daily candle's high must be >= low, and high >= close/open.
-- This test passes when it returns ZERO rows (dbt convention).

select
    symbol,
    trade_date,
    high_price,
    low_price,
    open_price,
    close_price
from {{ ref('fct_daily_price') }}
where high_price < low_price
   or high_price < open_price
   or high_price < close_price
   or low_price > open_price
   or low_price > close_price
