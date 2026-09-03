select
    id as customer_id,
    email,
    full_name,
    city,
    country_code,
    is_active,
    created_at
from {{ source('raw', 'customers') }}
