with
    selected_customer as (
        select
            customerid
        from {{ref('stg_sales_customer')}}
    )
    , selected_person as (
        select
            businessentityid
            , person_name
        from {{ref('stg_person_person')}}
    )
    , selected_businessentityaddress as (
        select
            businessentityid
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
    , transformation_customer as (
        select *
        from selected_customer
        left join selected_person on selected_customer.customerid = selected_person.businessentityid
    )
    , customer as (
        select *
        from transformation_customer
        where person_name is not null
    )
    , transformations as (
        select *
        from customer
        left join selected_businessentityaddress on customer.customerid = selected_businessentityaddress.businessentityid
        left join selected_personaddress on selected_businessentityaddress.address_id = selected_personaddress.addressid
        left join selected_personstateprovince on selected_personaddress.stateprovinceid = selected_personstateprovince.stateprovinceid
        left join selected_personcontryregion on selected_personstateprovince.countryregioncode = selected_personcontryregion.countryregioncode
    )
    , final as (
        select
            row_number() over (order by customerid) as customer_sk
            , customerid
            , addressid
            , person_name
            , city
            , state
            , country
        from transformations
    )
select * from final