{{config(
    materialized='table',
    partition_by= {
        "field": "ts",
        "date_type": "timestamp",
        "granularity": "hour"
    }
)}}

SELECT
    
FROM {{source("staging","listen_events")}}
LEFT JOIN {{ref("dim_users")}}
    ON listen_events.userId = dim_users.userId 
        AND CAST(listen_events.ts AS DATE) >= dim_users.rowActivationDate
        AND CAST(listen_events.ts AS DATE) < dim_users.rowExpirationDate
LEFT JOIN {{ref("dim_artist")}}
    ON REPLACE(REPLACE(listen_events.artist,'"',''), "\\","") = dim_artist.name
LEFT JOIN {{ref("dim_song")}}
    ON REPLACE(REPLACE(listen_events.artist,'"',''), "\\","") = dim_song.artist
        AND listen_events.song = dim_song.title
LEFT JOIN {{ref("dim_location")}}
    ON listen_events.city = dim_location.city
        AND listen_events.state = dim_location.stateCode
        AND listen_events.lat = dim_location.latitude
        AND listen_events.long = dim_location.longitude
LEFT JOIN {{ref("dim_date")}}
    ON dim_date.date = date_trunc(listen_events.ts, HOUR)