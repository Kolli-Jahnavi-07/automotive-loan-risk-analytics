select
    loan_id,
    cast(as_of_month as date) as as_of_month,
    scheduled_payment,
    payment_received,
    ending_balance,
    days_past_due,
    status,
    charge_off_amount,
    recovery_amount
from {{ source('silver', 'loan_performance') }}
