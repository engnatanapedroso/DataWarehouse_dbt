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
        where businessentityid is not null
    )
    , transformations as (
        select *
        from customer
        left join selected_businessentityaddress on customer.customerid = selected_businessentityaddress.businessentityid
    )
    , customer_address as (
        select *
        from transformations
        left join selected_personaddress on transformations.address_id = selected_personaddress.addressid
    )
    , customer_add as (
        select *
        from customer_address
        where addressid is not null
    )
    , customer_province as (
        select *
        from customer_address
        left join selected_personstateprovince on customer_address.stateprovinceid = selected_personstateprovince.stateprovinceid
    )
    , customer_country as (
        select *
        from customer_province
        left join selected_personcontryregion on customer_province.countryregioncode = selected_personcontryregion.countryregioncode
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
        from customer_country
    )
select * from final