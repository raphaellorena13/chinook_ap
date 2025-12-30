with source as (
    select
        "InvoiceId"::integer as invoice_id,
        "CustomerId"::integer as customer_id,
        "InvoiceDate"::timestamp as invoice_date,
        TRIM("BillingAddress") as billing_address,
        TRIM("BillingCity") as billing_city,
        TRIM("BillingState") as billing_state,
        TRIM("BillingCountry") as billing_country,
        TRIM("BillingPostalCode") as billing_postal_code,
        "Total"::numeric as total_amount
    from {{ source('raw', 'invoice') }}
)

select * from source