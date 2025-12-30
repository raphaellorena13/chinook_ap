with source as (
    select
        il.invoice_line_id,
        il.invoice_id,
        i.customer_id,
        il.track_id,
        i.invoice_date,
        il.quantity,
        il.unit_price,
        il.quantity * il.unit_price as line_amount
    from {{ ref('stg_invoiceline') }} il
    join {{ ref('stg_invoice') }} i
      on il.invoice_id = i.invoice_id
)

select * from source
