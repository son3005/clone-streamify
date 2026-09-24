{{ config(materialized='table')}}

SELECT {{ dbt_utils.surrogate_key(['latitude', 'longtitude','city', 'stateName'])}} as locationKey,
    *
FROM (
    SELECT
        distinct city,
        COALESCE(state_codes.stateCode,"NA") as stateCode,
        COALESCE(state_codes.stateName,"NA") as stateName,
        lat as latitude,
        lon as longitude
    FROM {{source("staging", "listen_events")}}
    LEFT JOIN {{ ref("state_codes")}} as listen_events.state = state_codes.state_codes
    UNION ALL 
    SELECT  'NA','NA','NA',0,0
)