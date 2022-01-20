with
    source_data as (
        select 
            salesorderid	
            , salesorderdetailid
            , productid
            , orderqty
            , unitprice
            , unitpricediscount
            , carriertrackingnumber
            , specialofferid
            , rowguid
            , modifieddate
        from {{source('adventure_works', 'sales_salesorderdetail')}}
)
select *
from source_data