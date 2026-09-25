{{config(materialized="table")}}

SELECT {{ dbt_utils.generate_surrogate_key(['songId']) }} as songKey,
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
        FROM {{ ref("songs")}}
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
) as songs