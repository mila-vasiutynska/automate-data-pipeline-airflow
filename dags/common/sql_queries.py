class SqlQueries:
    songplay_table_insert = ("""
    SELECT
        TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
        e.userId    AS user_id,
        e.level     AS level,
        s.song_id   AS song_id,
        a.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location  AS location,
        e.userAgent AS user_agent
    FROM staging_events e
    LEFT JOIN artists a
        ON a.name = e.artist
    LEFT JOIN songs s
        ON s.title = e.song
    AND s.artist_id = a.artist_id
    WHERE e.page = 'NextSong'
    AND e.ts IS NOT NULL;
    """)

    user_table_insert = ("""
    SELECT
        userId    AS user_id,
        firstName AS first_name,
        lastName  AS last_name,
        gender,
        level
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC, sessionId DESC, itemInSession DESC) AS rn
        FROM staging_events
        WHERE page = 'NextSong'
        AND userId IS NOT NULL
    ) t
    WHERE rn = 1;
    """)

    song_table_insert = ("""
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    """)

    artist_table_insert = ("""
    SELECT
        artist_id,
        artist_name      AS name,
        artist_location  AS location,
        artist_latitude  AS latitude,
        artist_longitude AS longitude
    FROM (
        SELECT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude,
            ROW_NUMBER() OVER (
                PARTITION BY artist_id
                ORDER BY artist_name DESC
            ) AS rn
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    ) t
    WHERE rn = 1;
    """)


    time_table_insert = ("""
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' AS start_time,
        EXTRACT(hour    FROM TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS hour,
        EXTRACT(day     FROM TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS day,
        EXTRACT(week    FROM TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS week,
        EXTRACT(month   FROM TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS month,
        EXTRACT(year    FROM TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS year,
        EXTRACT(weekday FROM TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
    AND ts IS NOT NULL;
    """)

