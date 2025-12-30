with source as (
    select
        "AlbumId"::integer as album_id,
        "Title" as title,
        "ArtistId"::integer as artist_id
    from {{ source('raw', 'album') }}
)

select * from source