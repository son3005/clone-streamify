{{
    config(
        materialized="view",
        partition_by={
            "field": "ts",
            "date_type": "timestamp",
            "granularity": "hour"
        }
    )
}}

SELECT
    fact_stream.userKey as userKey,
    fact_stream.aristKey as aristKey,
    fact_stream.songKey as songKey,
    fact_stream.dateKey as dateKey,
    fact_stream.locationKey as locationKey,
    fact_stream.ts as timestamp

    dim_users.firstName AS firstName,
    dim_users.lastName AS lastName,
    dim_users.gender AS gender,
    dim_users.level AS level,
    dim_users.userId as userId,
    dim_users.currentRow as currentUserRow,

    dim_songs.duration AS songDuration,
    dim_songs.tempo AS tempo,
    dim_songs.title AS songName,

    dim_location.city AS city,
    dim_location.stateName AS state,
    dim_location.latitude AS latitude,
    dim_location.longitude AS longitude,

    dim_datetime.date AS dateHour,
    dim_datetime.dayOfMonth AS dayOfMonth,
    dim_datetime.dayOfWeek AS dayOfWeek,
    
    dim_artists.latitude AS artistLatitude,
    dim_artists.longitude AS artistLongitude,
    dim_artists.name AS artistName

FROM {{ref("fact_streams")}}
JOIN {{ref("dim_users")}} on fact_streams.userKey = dim_users.userKey
JOIN {{ref("dim_songs")}} on fact_streams.songKey = dim_songs.songKey
JOIN {{ref("dim_location")}} on fact_streams.locationKey = dim_location.locationKey
JOIN {{ref("dim_datetime")}} on fact_streams.dateKey = dim_datetime.dateKey
JOIN {{ref("dim_artists")}} on fact_streams.artistKey = dim_artists.artistKey