with
    product as (
        select
            product_sk
            , productid
            , product_name
        from {{ref('dim_product')}}
    )
    , order_detail_with_sk as (
        select
            order_detail.salesorderdetailid
            , order_detail.salesorderid
            , product.product_sk as product_fk
            , order_detail.orderqty
            , order_detail.unitprice
        from {{ref('stg_sales_order_detail')}} as order_detail
        left join product on order_detail.productid = product.productid
    )
select * from order_detail_with_sk order by salesorderdetailid