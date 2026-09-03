select
    product_id,
    category_id,
    sku,
    name,
    price_amount,
    currency,
    in_stock,
    created_at
from {{ ref('stg_products') }}
