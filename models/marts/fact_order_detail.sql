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
    , product as (
        select
            product_sk
            , productid
            , product_name
        from {{ref('dim_product')}}
    )
    , order_with_sk as (
        select
            orders.salesorderid
            , customer.customer_sk
            , credit_card.creditcard_sk
            , orders.orderdate
            , orders.status
        from {{ref('stg_sales_order_header')}} as orders
        left join customer on orders.customerid = customer.customerid
        left join credit_card on orders.creditcardid = credit_card.creditcardid
    )
    , order_detail_with_sk as (
        select
            order_detail.salesorderid
            , product.product_sk
            , order_detail.orderqty
            , order_detail.unitprice
            , order_detail.unitpricediscount
        from {{ref('stg_sales_order_detail')}} as order_detail
        left join product on order_detail.productid = product.productid
    )
    , final as (
        select
            order_with_sk.salesorderid
            , order_with_sk.customer_sk as customer_fk
            , order_detail_with_sk.product_sk as product_fk
            , order_with_sk.creditcard_sk as creditcard_fk
            , order_detail_with_sk.orderqty
            , order_with_sk.orderdate
            , order_with_sk.status
            , order_detail_with_sk.unitprice
            , order_detail_with_sk.unitpricediscount

        from order_with_sk
        left join order_detail_with_sk on order_with_sk.salesorderid = order_detail_with_sk.salesorderid
    )
select * from final