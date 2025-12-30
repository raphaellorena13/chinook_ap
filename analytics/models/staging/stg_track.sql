with source as (
    select
        "TrackId"::integer as track_id,
        "Name" as name,
        "AlbumId"::integer as album_id,
        "MediaTypeId"::integer as media_type_id,
        "GenreId"::integer as genre_id,
        "Composer" as composer,
        "Milliseconds"::integer as milliseconds,
        "Bytes"::integer as bytes,
        "UnitPrice"::numeric as unit_price
    from {{ source('raw', 'track') }}
)

select * from source