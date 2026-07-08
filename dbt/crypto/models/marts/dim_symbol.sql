-- Dimension: one row per trading symbol. Small conformed dimension that both
-- fact tables join to. In a real project this would carry descriptive attributes
-- (asset name, category, launch date); here we derive what we can from the data.

with symbols as (

    select distinct symbol from {{ ref('stg_klines') }}
    union
    select distinct symbol from {{ ref('stg_trades') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['symbol']) }} as symbol_key,
    symbol,
    case
        when symbol = 'BTCUSDT' then 'Bitcoin'
        when symbol = 'ETHUSDT' then 'Ethereum'
        when symbol = 'SOLUSDT' then 'Solana'
        when symbol = 'ADAUSDT' then 'Cardano'
        else symbol
    end                              as asset_name,
    'crypto'                         as asset_class,
    'USDT'                           as quote_currency
from symbols
