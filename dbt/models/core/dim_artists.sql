{{ config(materialized='table')}}
SELECT {{ dbt_utils.generate_surrogate_key(['artistId']) }} as artistKey,
    *
FROM (
    SELECT
        MAX(artist_id) as artistId,
        MAX(artist_latitude) as latitude,
        MAX(artist_longitude) as longitude,
        MAX(artist_location) as location,
        REPLACE(REPLACE(artist_name, '"', ''), '\\', '') as name
    FROM {{ ref("songs") }}
    GROUP BY artist_name
    UNION ALL
    SELECT  'NNNNNNNNNNNNNNN',
            0,
            0,
            'NA',
            'NA'
) as artists