with source as (
    select
        "CustomerId"::integer as customer_id,
        TRIM("FirstName") as first_name,
        TRIM("LastName") as last_name,
        TRIM("Company") as company,
        TRIM("Address") as address,
        TRIM("City") as city,
        TRIM("State") as state,
        TRIM("Country") as country,
        TRIM("PostalCode") as postal_code,
        TRIM("Phone") as phone,
        TRIM("Fax") as fax,
        TRIM("Email") as email,
        "SupportRepId"::integer as support_rep_id
    from {{ source('raw', 'customer') }}
)

select * from source