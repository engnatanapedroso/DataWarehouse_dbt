with
    product as (
        select
            product_sk
            , productid as product_id
            , product_name
        from {{ref('dim_product')}}
    )
    , order_detail as (
        select
            salesorderid as order_id
            , productid
        from {{ref('stg_sales_order_detail')}} as order_detail
        left join product on order_detail.productid = product.product_id
    )
    select * from order_detail