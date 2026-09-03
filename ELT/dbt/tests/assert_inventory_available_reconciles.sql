-- Same check as ETL's validate_inventory_available(), now in SQL.
select
    inventory_id,
    on_hand,
    reserved,
    available
from {{ ref('fact_inventory_snapshot') }}
where available != on_hand - reserved
