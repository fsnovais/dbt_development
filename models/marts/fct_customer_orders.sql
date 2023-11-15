with

    -- Import CTEs
    orders as (select * from {{ ref('stg_jaffle_shop__orders') }}),

    customers as (select * from {{ ref('stg_jaffle_shop__customers') }}),

    payments as (select * from {{ ref('stg_stripe__payments') }}),

    -- Logical CTEs

    -- Marts
    customer_order_history as (
        (

            select
                b.customer_id,
                b.full_name,
                b.surname,
                b.givenname,
                min(a.order_date) as first_order_date,

                min(
                    case
                        when a.order_status not in ('returned', 'return_pending')
                        then a.order_date
                    end
                ) as first_non_returned_order_date,

                max(
                    case
                        when a.order_status not in ('returned', 'return_pending')
                        then a.order_date
                    end
                ) as most_recent_non_returned_order_date,

                coalesce(max(user_order_seq), 0) as order_count,

                coalesce(
                    count(case when a.order_status != 'returned' then 1 end), 0
                ) as non_returned_order_count,
                sum(
                    case
                        when a.order_status not in ('returned', 'return_pending')
                        then c.payment_ammount
                        else 0
                    end
                ) as total_lifetime_value,

                sum(
                    case
                        when a.order_status not in ('returned', 'return_pending')
                        then c.payment_ammount
                        else 0
                    end
                ) / nullif(
                    count(
                        case
                            when a.order_status not in ('returned', 'return_pending') then 1
                        end
                    ),
                    0
                ) as avg_non_returned_order_value,
                array_agg(distinct a.order_id) as order_ids

            from orders a

            join customers b on a.customer_id = b.customer_id

            left outer join payments c on a.order_id = c.order_id

            where a.order_status not in ('pending') and c.payment_status != 'fail'

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
            c.payment_ammount as order_value_dollars,
            a.order_status,
            c.payment_status

        from orders a

        join customers b on a.customer_id = b.customer_id

        join
            customer_order_history
            on a.customer_id = customer_order_history.customer_id

        left outer join payments c on a.order_id = c.order_id

        where c.payment_status != 'fail'

    )
-- Simple Select Statement
select *
from final
