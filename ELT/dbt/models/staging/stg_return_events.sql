select
    id as return_id,
    event.status as status,
    event.message as message,
    event.createdAt as created_at
from {{ source('raw', 'returns') }},
unnest(events) as event
