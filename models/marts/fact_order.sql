with
    customer as (
        select
            customer_sk
            , customerid
        from {{ref('dim_customer')}}
    )
    , credit_card as (
        select
            creditcard_sk
            , creditcardid
            , cardtype
        from {{ref('dim_sales')}}
    )
    , order_with_sk as (
        select
            orders.salesorderid
            , customer.customer_sk as customer_fk
            , credit_card.creditcard_sk as creditcard_fk
            , orders.orderdate
            , orders.status
            , orders.totaldue
        from {{ref('stg_sales_order_header')}} as orders
        left join customer on orders.customerid = customer.customerid
        left join credit_card on orders.creditcardid = credit_card.creditcardid
    )
select * from order_with_sk