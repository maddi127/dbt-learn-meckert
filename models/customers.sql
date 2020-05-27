{{
  config(
    materialized='view'
  )
}}

with customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from raw.jaffle_shop.customers

),

orders as (
    select * from {{ ref('orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from {{ ref('stg_orders') }}

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        sum(coalesce(orders.payment_total,0)) as customer_lifetime_value

    from customers

    left join customer_orders using (customer_id)
    left join orders using (customer_id)

    group by 1,2,3,4,5,6

)

select * from final