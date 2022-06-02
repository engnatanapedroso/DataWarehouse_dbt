with
    source_data as (
        select 
            countryregioncode
            , name as country
            , modifieddate
        from {{source('adventure_works', 'person_countryregion')}}
)
select *
from source_data