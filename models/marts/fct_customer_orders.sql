with

    orders as (
        select * from {{ ref('int_orders') }}
    ),

    customers as (select * from {{ ref("stg_jaffle_shop__customers") }}),

    customer_order_history as (
        (

            select
                b.customer_id,
                b.full_name,
                b.surname,
                b.givenname,
                min(a.order_date) as first_order_date,

                min(a.valid_order_date) as first_non_returned_order_date,

                max(a.valid_order_date) as most_recent_non_returned_order_date,

                coalesce(max(a.user_order_seq), 0) as order_count,

                coalesce(
                    count(case when a.valid_order_date is not null then 1 end), 0
                ) as non_returned_order_count,
                sum(
                    case
                        when a.valid_order_date is not null
                        then a.order_value_dollars
                        else 0
                    end
                ) as total_lifetime_value,

                sum(
                    case
                        when a.valid_order_date is not null
                        then a.order_value_dollars
                        else 0
                    end
                ) / nullif(
                    count(case when a.valid_order_date is not null then 1 end), 0
                ) as avg_non_returned_order_value,
                array_agg(distinct a.order_id) as order_ids

            from orders a

            join customers b on a.customer_id = b.customer_id

            group by b.customer_id, b.full_name, b.surname, b.givenname

        )
    ),
    -- Final CTE
    final as (
        select

            a.order_id,
            a.customer_id,
            b.surname,
            b.givenname,
            first_order_date,
            order_count,
            total_lifetime_value,
            a.order_value_dollars,
            a.order_status,
            a.payment_status

        from orders a

        join customers b on a.customer_id = b.customer_id

        join
            customer_order_history on a.customer_id = customer_order_history.customer_id

    )
-- Simple Select Statement
select *
from final
