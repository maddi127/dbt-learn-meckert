{% set payment_methods_query %}
select distinct payment_method from {{ ref('stg_payments') }}
order by 1
{% endset %}

{% set results = run_query(payment_methods_query) %}

{% if execute %}
{# Return the first column #}

{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

with payments as (

    select *
    from {{ ref('stg_payments')}}
),

amounts as (

    select
    order_id,
    {% for pm in results_list %}
    sum( case when payment_method = '{{ pm }}' then amount_usd else 0 end ) as {{ pm }}_amount
    {%- if not loop.last -%} , {%- endif -%}
    {% endfor %}
    from payments
    group by 1
)

select *
from amounts