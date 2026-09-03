select
    id as category_id,
    slug,
    name
from {{ source('raw', 'categories') }}
