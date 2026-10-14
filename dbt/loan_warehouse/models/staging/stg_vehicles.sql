select
    vin,
    make,
    model,
    cast(model_year as integer) as model_year,
    condition as vehicle_condition,
    cast(vehicle_value as double) as vehicle_value
from {{ source('silver', 'vehicles') }}
