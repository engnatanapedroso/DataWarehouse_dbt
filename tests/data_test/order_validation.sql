-- ORDER VALIDATION DATA TEST
-- SELECT distinct count (salesorderid) FROM `adventure-works-337512.Adventure_works_source_kondado.sales_salesorderheader` 
--where orderdate between '2013-01-31' and '2013-03-31' LIMIT 1000
with
    order_validation as (
        select distinct count (salesorderid) as orders
        from {{ref('fact_order_detail')}}
        where orderdate between '2013-01-31' and '2013-03-31'
    )
select * from order_validation where orders != 772