with
    source_data as (
        select 
            salesorderid
            , salesreasonid
            , modifieddate
        from {{source('adventure_works', 'sales_salesorderheadersalesreason')}}
)
select *
from source_data