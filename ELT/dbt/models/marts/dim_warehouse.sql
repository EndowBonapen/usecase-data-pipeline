select distinct
    warehouse_id,
    warehouse_name
from {{ ref('stg_inventory') }}
where warehouse_id is not null
