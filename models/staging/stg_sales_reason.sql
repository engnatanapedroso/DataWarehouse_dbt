with
    source_data as (
        select 
            salesreasonid
            , name as reason
            , reasontype
            , modifieddate
        from {{source('adventure_works', 'sales_salesreason')}}
)
select *
from source_data