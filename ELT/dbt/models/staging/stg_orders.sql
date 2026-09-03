select
    id as order_id,
    customer_id,
    order_number,
    status,
    currency,
    cast(total_minor as numeric) / 100 as total_amount,
    placed_at,
    shipped_at
from {{ source('raw', 'orders') }}
