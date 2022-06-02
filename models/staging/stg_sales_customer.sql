with
    source_data as (
        select 
            customerid
            , personid
            , storeid
            , territoryid
            , rowguid
            , modifieddate
        from {{source('adventure_works', 'sales_customer')}}
)
select *
from source_data