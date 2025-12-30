with source as (
    select
        "GenreId"::integer as genre_id,
        TRIM("Name") as name
    from {{ source('raw', 'genre') }}
)

select * from source
