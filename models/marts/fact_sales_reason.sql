with
    order_products as (
        select
            order_id
            , productid
        from {{ref('factless_fact_table')}}
    )
    , selected_salesorderheadersalesreason as (
        select
            salesorderid
            , salesreasonid
        from {{ref('stg_sales_order_header_sales_reason')}}
    )
    , selected_salesreason as (
        select
            salesreasonid as salesreason_id
            , reason
            , reasontype
        from {{ref('stg_sales_reason')}}
    )
    , transformed as (
        select *
        from selected_salesorderheadersalesreason
        left join order_products on selected_salesorderheadersalesreason.salesorderid = order_products.order_id
        left join selected_salesreason on selected_salesorderheadersalesreason.salesreasonid = selected_salesreason.salesreason_id
    )
    , final as (
        select
            salesorderid
            , salesreasonid
            , productid
            , reason
            , reasontype
        from transformed
    )
select * from final