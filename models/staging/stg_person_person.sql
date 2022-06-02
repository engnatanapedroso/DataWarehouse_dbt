with
    source_data as (
        select 
            businessentityid
            , concat (firstname, ' ', lastname) as person_name
            , persontype
            , namestyle
            , title
            , firstname
            , middlename
            , lastname
            , suffix
            , emailpromotion
            , additionalcontactinfo
            , demographics
            , rowguid
            , modifieddate
        from {{source('adventure_works', 'person_person')}}
)
select *
from source_data