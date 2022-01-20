with
    creditcard as (
        select
            creditcardid
            , cardtype
        from {{ref('stg_sales_credit_card')}}
    )
    , transformation as (
        select
            row_number() over (order by creditcardid) as creditcard_sk
            , creditcardid
            , cardtype
        from creditcard
    )
select * from transformation