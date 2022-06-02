with
    source_data as (
        select 
            businessentityid
            , addressid
            , addresstypeid
            , rowguid
            , modifieddate
        from {{source('adventure_works', 'person_businessentityaddress')}}
)
select *
from source_data