select
    borrower_id,
    credit_score,
    credit_tier,
    annual_income,
    case
        when annual_income is null then 'unknown'
        when annual_income < 40000 then '< 40k'
        when annual_income < 75000 then '40-75k'
        when annual_income < 125000 then '75-125k'
        else '125k+'
    end as income_band,
    state as borrower_state,
    employment_years
from {{ ref('stg_borrowers') }}
