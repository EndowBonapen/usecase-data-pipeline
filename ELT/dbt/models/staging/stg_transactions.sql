select
    id as transaction_id,
    amount,
    currency,
    type as transaction_type,
    status,
    description,
    merchant,
    timestamp as occurred_at,
    reference
from {{ source('raw', 'transactions') }}
