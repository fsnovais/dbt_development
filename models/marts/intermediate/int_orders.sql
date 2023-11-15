with 

orders as (select * from {{ ref("stg_jaffle_shop__orders") }}),

    payments as (
        select * from {{ ref("stg_stripe__payments") }} where payment_status != 'fail'
    ),

order_totals as (
    select order_id, payment_status, sum(payment_ammount) as order_value_dollars

    from payments
    group by 1, 2
),

order_values_joined as (
    select a.*, b.payment_status, b.order_value_dollars

    from orders a
    left join order_totals b on a.order_id = b.order_id
)

select * from order_values_joined