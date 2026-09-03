select category_id, slug, name
from {{ ref('stg_categories') }}
