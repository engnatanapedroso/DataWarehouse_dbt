-- ORDER QUANTITY VALIDATION DATA TEST
-- SELECT sum (orderqty) FROM `adventure-works-337512.Adventure_works_source_kondado.sales_salesorderdetail` LIMIT 1000
with
    order_qty_validation as (
        select sum (orderqty) as quantity
        from {{ref('fact_order_detail')}}
    )
select * from order_qty_validation where quantity != 274914