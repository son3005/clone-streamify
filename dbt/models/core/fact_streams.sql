{{ config(materialized='table') }}

SELECT
    dim_users.userKey,
    dim_artists.artistKey,
    dim_songs.songKey,
    dim_datetime.dateKey,
    dim_location.locationKey,
    listen_events.ts
FROM {{ source("staging", "listen_events") }}
LEFT JOIN {{ ref("dim_users") }}
    ON listen_events.userId = dim_users.userId 
        AND CAST(listen_events.ts AS DATE) >= dim_users.rowActivationDate
        AND CAST(listen_events.ts AS DATE) < dim_users.rowExpirationDate
LEFT JOIN {{ ref("dim_artists") }}
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_artists.name
LEFT JOIN {{ ref("dim_songs") }}
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_songs.artistName
        AND listen_events.song = dim_songs.title
LEFT JOIN {{ ref("dim_location") }}
    ON listen_events.city = dim_location.city
        AND listen_events.state = dim_location.stateCode
        AND listen_events.lat = dim_location.latitude
        AND listen_events.lon = dim_location.longitude
LEFT JOIN {{ ref("dim_datetime") }}
    ON dim_datetime.date = date_trunc('hour', listen_events.ts)
