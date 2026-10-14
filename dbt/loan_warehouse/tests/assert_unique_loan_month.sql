-- fct_loan_month grain: exactly one row per loan per month.
select loan_id, as_of_month, count(*) as n
from {{ ref('fct_loan_month') }}
group by loan_id, as_of_month
having count(*) > 1
