with source as (
    select
        "ArtistId"::integer as artist_id,
        "Name" as name
    from {{ source('raw', 'artist') }}
)

select * from source