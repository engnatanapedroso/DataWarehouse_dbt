with
    source_data as (
        select 
            salesorderid
            , customerid
            , creditcardid
            , salespersonid
            , shiptoaddressid
            , territoryid
            , billtoaddressid
            , shipmethodid
            , currencyrateid
            , status
            , orderdate
            , duedate
            , shipdate
            , subtotal
            , taxamt
            , freight
            , totaldue
            , comment
            , revisionnumber
            , onlineorderflag
            , purchaseordernumber
            , accountnumber
            , creditcardapprovalcode
            , rowguid
            , modifieddate
        from {{source('adventure_works', 'sales_salesorderheader')}}
)
select *
from source_data