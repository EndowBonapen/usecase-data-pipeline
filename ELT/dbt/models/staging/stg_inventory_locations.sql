select
    inv.id as inventory_id,
    location.id as location_id,
    location.onHand as on_hand,
    location.reserved as reserved,
    location.available as available
from {{ source('raw', 'inventory') }} as inv,
unnest(locations) as location
