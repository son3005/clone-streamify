{{config(materialized="table")}}

SELECT {{ dbt_utils.generate_surrogate_key(['userId','rowActivationDate','level']) }} as userKey,    *
FROM 
(
   SELECT 
        CAST(userId as BIGINT) as userId,
        firstName,
        lastName,
        gender,
        level,
        CAST(registration as BIGINT) as registration,
        minDate as rowActivationDate,
        LEAD(minDate, 1, DATE '9999-12-31') OVER (
            PARTITION BY 
                userId,
                firstName,
                lastName,
                gender
            ORDER BY grouped
        ) as rowExpirationDate,
        CASE WHEN RANK() OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped desc) = 1 THEN 1 ELSE 0 END AS currentRow
   FROM
   (
        SELECT 
            userId, 
            firstName, 
            lastName, 
            gender, 
            registration, 
            level, 
            grouped, 
            cast(min(date) as date) as minDate
        FROM
        (
            SELECT 
                *,
                SUM(lagged) OVER (PARTITION BY userId, firstName, lastName, gender ORDER BY date) as grouped
            FROM
            (
                SELECT
                    *,
                    CASE WHEN LAG(level,1,'NA') OVER(PARTITION BY userId, firstName,lastName, gender ORDER BY date) <> level THEN 1 ELSE 0 END as lagged
                FROM
                (
                    SELECT
                        distinct userId,
                        firstName,
                        lastName,
                        gender,
                        registration,
                        level,
                        ts as date
                    FROM {{source("staging","listen_events")}}
                    WHERE userId <> 0 
                ) as t0
            ) as t1
        ) as t2
        GROUP BY  userId, 
                firstName,
                lastName,
                gender,
                level,
                registration,
                grouped
   ) as t3
    UNION ALL 
    SELECT 
        CAST(userId as BIGINT) as userKey,
        firstName,
        lastName,
        gender,
        level,
        CAST(registration as BIGINT) as registration,
        CAST(min(ts) as date) as rowActivationDate,
        DATE '9999-12-31' as rowExpirationDate,
        1 as currentRow
    FROM {{source("staging","listen_events")}}
    WHERE userId = 1 or userId = 0
    GROUP BY userId, firstName, lastName, gender, level, registration
) as users
