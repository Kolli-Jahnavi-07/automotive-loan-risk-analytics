-- A loan cannot have a servicing snapshot before the month after it was originated.
select m.loan_id, m.as_of_month, o.origination_date
from {{ ref('fct_loan_month') }} m
join {{ ref('fct_loan_origination') }} o using (loan_id)
where m.as_of_month <= o.origination_month
