with
    source_data as (
        select 
            addressid
            , stateprovinceid
            , city
            , addressline1
            , addressline2
            , postalcode
            , spatiallocation
            , rowguid
            , modifieddate
        from {{source('adventure_works', 'person_address')}}
)
select *
from source_data