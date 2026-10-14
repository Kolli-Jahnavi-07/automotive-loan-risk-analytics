select
    loan_id,
    borrower_id,
    dealer_id,
    vin,
    cast(origination_date as date) as origination_date,
    amount_financed,
    apr,
    term_months,
    monthly_payment
from {{ source('silver', 'loans') }}
