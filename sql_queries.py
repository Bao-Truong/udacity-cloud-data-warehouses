import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']["ARN"]
LOG_DATA = config['S3']["LOG_DATA"]
LOG_JSONPATH = config['S3']["LOG_JSONPATH"]
SONG_DATA = config['S3']["SONG_DATA"]
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          TEXT ,
    auth            TEXT , 
    firstName       TEXT ,
    gender          TEXT , 
    itemInSession   INT ,
    lastName        TEXT , 
    length          DOUBLE PRECISION , 
    level           TEXT ,
    location        TEXT ,
    method          TEXT ,
    page            TEXT ,
    registration    TEXT ,
    sessionId       INT ,
    song            TEXT ,
    status          INT ,
    ts              BIGINT ,
    userAgent       TEXT ,
    userId          INT 
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT ,
    artist_id           TEXT , 
    artist_latitude     DOUBLE PRECISION, 
    artist_longitude    DOUBLE PRECISION, 
    artist_location     TEXT ,
    artist_name         TEXT , 
    song_id             TEXT , 
    title               TEXT ,
    duration            FLOAT(8) ,
    year                INT 
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL, 
    level TEXT, 
    song_id TEXT, 
    artist_id TEXT, 
    session_id INT, 
    location TEXT, 
    user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT  , 
    first_name TEXT, 
    last_name TEXT, 
    gender TEXT, 
    level TEXT
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT  , 
    title TEXT NOT NULL, 
    artist_id TEXT NOT NULL, 
    year INT, 
    duration FLOAT(8) NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT , 
    name TEXT NOT NULL, 
    location TEXT, 
    latitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP , 
    hour SMALLINT, 
    day SMALLINT, 
    week SMALLINT, 
    month SMALLINT, 
    year SMALLINT, 
    weekday SMALLINT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
json {} region 'us-west-2';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
json 'auto ignorecase' region 'us-west-2';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES( %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES(%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
VALUES(%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
VALUES(%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
VALUES(%s, %s, %s, %s, %s, %s, %s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
