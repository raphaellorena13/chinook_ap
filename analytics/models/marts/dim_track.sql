with source as (
    select
        t.track_id,
        t.name as track_name,
        g.name as genre_name,
        a.name as artist_name,
        al.title as album_title,
        t.milliseconds,
        t.composer
    from {{ ref('stg_track') }} t
    left join {{ ref('stg_genre') }} g on t.genre_id = g.genre_id
    left join {{ ref('stg_album') }} al on t.album_id = al.album_id
    left join {{ ref('stg_artist') }} a on al.artist_id = a.artist_id
)

select * from source
