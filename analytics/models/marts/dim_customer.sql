with source as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        country,
        city,
        state,
        company
    from {{ ref('stg_customer') }}
)

select * from source
