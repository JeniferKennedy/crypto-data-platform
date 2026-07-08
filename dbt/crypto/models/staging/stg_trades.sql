-- Staging: clean + type the raw trade stream. One row per trade event.
-- Staging models do light cleanup only (casting, renaming, dedupe) — no business logic.

with source as (

    select * from {{ source('raw', 'trades') }}

),

cleaned as (

    select
        event_id,
        upper(symbol)                as symbol,
        price::numeric               as price,
        quantity::numeric            as quantity,
        lower(side)                  as side,
        event_time::timestamptz      as event_time,
        (price * quantity)::numeric  as notional_value
    from source
    where event_id is not null
      and price > 0
      and quantity > 0

),

deduped as (

    -- Same event_id can arrive twice with at-least-once delivery; keep the first.
    select *,
           row_number() over (partition by event_id order by event_time) as rn
    from cleaned

)

select
    event_id,
    symbol,
    price,
    quantity,
    side,
    notional_value,
    event_time
from deduped
where rn = 1
