
with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

final as (
    select 
    orders.order_id as order_id,
    orders.customer_id as customer_id,
    orders.status as order_status,
    orders.order_date as order_date,
    sum(payments.amount_usd) as payment_total,
    count(distinct payments.payment_id) as payment_count
    
    from orders

    left join payments using (order_id)
    group by 1,2,3,4
    
)

select * from final