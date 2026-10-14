select
    borrower_id,
    credit_score,
    annual_income,
    state,
    employment_years,
    {{ credit_tier('credit_score') }} as credit_tier
from {{ source('silver', 'borrowers') }}
