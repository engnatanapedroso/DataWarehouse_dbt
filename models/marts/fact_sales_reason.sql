with
    selected_salesreason as (
        select
            salesreasonid as salesreason_id
            , reason
            , reasontype
        from {{ref('stg_sales_reason')}}
    )
    , selected_salesorderheadersalesreason as (
        select
            salesreasonid
            , salesorderid
        from {{ref('stg_sales_order_header_sales_reason')}}
    )
    , transformed as (
        select *
        from selected_salesorderheadersalesreason
        left join selected_salesreason on selected_salesorderheadersalesreason.salesreasonid = selected_salesreason.salesreason_id
    )
    , final as (
        select
            row_number() over (order by salesreason_id) as sales_reason_sk
            , salesreason_id
            , salesorderid
            , reason
            , reasontype
        from transformed
    )
select * from final