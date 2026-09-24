{{ config(materialized='table')}}

SELECT {{ dbt_utils.surrogate_key(['artistId'])}} as artistKey,
    *
FROM (
    SELECT
        MAX(artist_id) as artistId,
        MAX(artist_latitude) as latitude,
        MAX(artist_longitude) AS longitude,
        MAX(artist_location) AS location,
        PLACE(REPLACE(artist_name, '"', ''), '\\', '') AS name
    FROM {{ source("staging", "song")}}
    GROUP BY artist_name
    UNION ALL
    SELECT  'NNNNNNNNNNNNNNN',
            0,
            0,
            'NA',
            'NA'
)
