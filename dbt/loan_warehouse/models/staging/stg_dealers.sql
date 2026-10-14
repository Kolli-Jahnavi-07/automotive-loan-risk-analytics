select
    dealer_id,
    dealer_name,
    state as dealer_state,
    dealer_type
from {{ source('silver', 'dealers') }}
