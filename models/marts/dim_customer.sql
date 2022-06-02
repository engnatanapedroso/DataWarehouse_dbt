with
    selected_customer as (
        select
            customerid
        from {{ref('stg_sales_customer')}}
    )
    ,selected_person as (
        select
            businessentityid
            , person_name
        from {{ref('stg_person_person')}}
    )
    , selected_businessentityaddress as (
        select
            businessentityid as businessentity_id
            , addressid as address_id
        from {{ref('stg_person_business_entity_address')}}
    )
    , selected_personaddress as (
        select
            addressid
            , stateprovinceid
            , city
        from {{ref('stg_person_address')}}
    )
    , selected_personstateprovince as (
        select
            stateprovinceid
            , territoryid
            , state
            , countryregioncode
        from {{ref('stg_person_state_province')}}
    )
    , selected_personcontryregion as (
        select
            countryregioncode
            , country
        from {{ref('stg_person_country_region')}}
    )
    , transformation as (
        select * 
        from selected_customer
        full join selected_person on selected_customer.customerid = selected_person.businessentityid
        left join selected_businessentityaddress on selected_person.businessentityid = selected_businessentityaddress.businessentity_id
        left join selected_personaddress on selected_businessentityaddress.address_id = selected_personaddress.addressid
        left join selected_personstateprovince on selected_personaddress.stateprovinceid = selected_personstateprovince.stateprovinceid
        left join selected_personcontryregion on selected_personstateprovince.countryregioncode = selected_personcontryregion.countryregioncode
    )
    , customer as (
        select
            customerid
            , person_name
            , city
            , state
            , country
        from transformation
    )
    , final as (
        select
            row_number () over (order by customerid) as customer_sk
            , customerid
            , person_name
            , city
            , state
            , country
        from customer
    )
select * from final