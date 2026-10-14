-- Recoveries can never exceed the amount charged off.
select loan_id, as_of_month, charge_off_amount, recovery_amount
from {{ ref('fct_loan_month') }}
where recovery_amount > charge_off_amount
