CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          DOUBLE PRECISION,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR(65535),
    userId          INTEGER
);

CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DOUBLE PRECISION,
    year             INTEGER
);

CREATE TABLE IF NOT EXISTS songplays (
    songplay_id VARCHAR PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    userid      INTEGER,
    level       VARCHAR,
    song_id     VARCHAR,
    artist_id   VARCHAR,
    sessionid   INTEGER,
    location    VARCHAR,
    useragent   VARCHAR
);

CREATE TABLE IF NOT EXISTS users (
    userid    INTEGER PRIMARY KEY,
    firstname VARCHAR,
    lastname  VARCHAR,
    gender    VARCHAR(1),
    level     VARCHAR
);

CREATE TABLE IF NOT EXISTS songs (
    song_id    VARCHAR PRIMARY KEY,
    title      VARCHAR,
    artist_id  VARCHAR,
    year       INTEGER,
    duration   FLOAT
);

CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR PRIMARY KEY,
    artist_name     VARCHAR,
    artist_location VARCHAR,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION
);


CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
);