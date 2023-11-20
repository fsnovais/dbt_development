with

    orders as (
        select * from {{ ref('int_orders') }}
    ),

    customers as (select * from {{ ref("stg_jaffle_shop__customers") }}),

    customer_orders as (
        select
            a.*,

            b.full_name,
            b.surname,
            b.givenname,

            -- Customer Leevel agregation
            min(a.order_date) over(
                partition by a.customer_id
            ) as customer_first_order_date,

            min(a.valid_order_date) over(
                partition by a.customer_id
            ) as customer_first_non_returned_order_date,

            max(a.valid_order_date) over(
                partition by a.customer_id
            ) as customer_most_recent_non_returned_order_date,

            count(*) over(
                partition by a.customer_id
            ) as customer_order_count,

            sum(if(a.valid_order_date is not null, 1, 0)) over(
                partition by a.customer_id
            ) as customer_non_returned_order_count,

            sum(if(a.valid_order_date is not null, a.order_value_dollars, 0)) over(
                partition by a.customer_id
            ) as customer_total_lifetime_value,

            (select array_agg(distinct order_id) from unnest([a.order_id]) as order_id) as customer_order_ids

        from orders a
        inner join customers b
            on a.customer_id = b.customer_id
    ),
    add_avg_order_values as (
        select *,
        safe_divide(customer_total_lifetime_value, customer_non_returned_order_count) as customer_avg_non_returned_order_value

        from customer_orders
    ),
    -- Final CTE
    final as (
        select

            order_id,
            customer_id,
            surname,
            givenname,
            customer_first_order_date as first_order_date,
            customer_order_count as order_count,
            customer_total_lifetime_value as total_lifetime_value,
            order_value_dollars,
            order_status,
            payment_status

        from add_avg_order_values

    )
-- Simple Select Statement
select *
from final
