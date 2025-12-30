with source as (
    select
        "InvoiceLineId"::integer as invoice_line_id,
        "InvoiceId"::integer as invoice_id,
        "TrackId"::integer as track_id,
        "UnitPrice"::numeric as unit_price,
        "Quantity"::integer as quantity
    from {{ source('raw', 'invoiceline') }}
)

select * from source