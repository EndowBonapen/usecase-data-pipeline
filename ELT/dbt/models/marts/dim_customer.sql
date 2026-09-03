select
    customer_id,
    email,
    full_name,
    city,
    country_code,
    is_active,
    created_at
from {{ ref('stg_customers') }}
