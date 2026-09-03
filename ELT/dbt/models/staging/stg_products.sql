select
    id as product_id,
    category_id,
    sku,
    name,
    cast(price_minor as numeric) / 100 as price_amount,
    currency,
    in_stock,
    created_at
from {{ source('raw', 'products') }}
