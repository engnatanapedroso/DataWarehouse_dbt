with
    products as (
        select
            productid
            , product_name
        from {{ref('stg_production_product')}}
    )
    , transformation as (
        select
            row_number () over (order by productid) as product_sk
            , productid
            , product_name
        from products
    )
select * from transformation