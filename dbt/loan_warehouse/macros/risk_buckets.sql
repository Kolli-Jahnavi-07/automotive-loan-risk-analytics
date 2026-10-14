{% macro credit_tier(score_col) -%}
    case
        when {{ score_col }} >= 720 then 'super_prime'
        when {{ score_col }} >= 660 then 'prime'
        when {{ score_col }} >= 620 then 'near_prime'
        else 'subprime'
    end
{%- endmacro %}

{% macro ltv_band(ltv_col) -%}
    case
        when {{ ltv_col }} < 0.80 then '< 80%'
        when {{ ltv_col }} < 1.00 then '80-100%'
        when {{ ltv_col }} < 1.20 then '100-120%'
        else '120%+'
    end
{%- endmacro %}

{% macro dpd_bucket(dpd_col, status_col) -%}
    case
        when {{ status_col }} = 'charged_off' then 'charged_off'
        when {{ status_col }} = 'paid_off' then 'paid_off'
        when {{ dpd_col }} >= 90 then '90+'
        when {{ dpd_col }} >= 60 then '60-89'
        when {{ dpd_col }} >= 30 then '30-59'
        else 'current'
    end
{%- endmacro %}
