select
    id as order_item_id,
    order_id,
    product_id,
    quantity,
    cast(unit_price_minor as numeric) / 100 as unit_price_amount,
    cast(line_total_minor as numeric) / 100 as line_total_amount
from {{ source('raw', 'order_items') }}
