{{config(materialized="table")}}

SELECT {{dbt_utils.surrogate_key(['songId'])}} as songKey,
    *
FROM (
    (
        SELECT song_id as songId,
            REPLACE(REPLACE(artist_name,'"',''),'\\','') as artistName,
            duration,
            key,
            key_confidence as KeyConfidence,
            loudness,
            song_hotttnesss as songHotness,
            tempo,
            title,
            year 
        FROM {{ source("staging","songs")}}
    )
    UNION ALL 
    SELECT 'NNNNNNNNNNNNNNNNNNN',
                'NA',
                0,
                -1,
                -1,
                -1,
                -1,
                -1,
                'NA',
                0
)