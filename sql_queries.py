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
    start_time  TIMESTAMP NOT NULL SORTKEY,
    user_id     INT NOT NULL DISTKEY, 
    level       TEXT NOT NULL, 
    song_id     TEXT , 
    artist_id   TEXT , 
    session_id  INT NOT NULL, 
    location    TEXT NOT NULL, 
    user_agent  TEXT NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     INT NOT NULL SORTKEY DISTKEY, 
    first_name  TEXT NOT NULL, 
    last_name   TEXT NOT NULL, 
    gender      TEXT NOT NULL, 
    level       TEXT NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     TEXT   NOT NULL SORTKEY, 
    title       TEXT NOT NULL, 
    artist_id   TEXT NOT NULL, 
    year        INT NOT NULL, 
    duration    FLOAT(8) NOT NULL
)diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   TEXT  NOT NULL SORTKEY, 
    name        TEXT NOT NULL, 
    location    TEXT, 
    latitude    DOUBLE PRECISION, 
    longitude   DOUBLE PRECISION
)diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP  NOT NULL SORTKEY, 
    hour        SMALLINT NOT NULL,  
    day         SMALLINT NOT NULL, 
    week        SMALLINT NOT NULL, 
    month       SMALLINT NOT NULL, 
    year        SMALLINT NOT NULL, 
    weekday     SMALLINT NOT NULL
)diststyle all;
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
SELECT
	timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time ,
	se.userid ,
	se."level",
	ss.song_id ,
	ss.artist_id, 
	se.sessionid ,
	se."location" ,
	se.useragent
FROM
	staging_events se
LEFT JOIN staging_songs ss 
ON
    se.artist = ss.artist_name
	and se.song = ss.title
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
select	
	se.userid ,
	se.firstname ,
	se.lastname ,
	se.gender ,
	se."level" 
from
	staging_events se
where se.userid is not null
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
select	
	ss.song_id ,
	ss.title ,
	ss.artist_id ,
	ss."year" ,
	ss.duration 
from
	staging_songs ss 
where ss.song_id is not null
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
select	
	ss.artist_id ,
	ss.artist_name ,
	ss.artist_location ,
	ss.artist_latitude ,
	ss.artist_longitude 
from
	staging_songs ss 
where ss.artist_id is not null
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
select	
	timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time ,
	extract(hour from start_time) as hour,
	extract(day from start_time) as day,
	extract(week from start_time) as week,
	extract(month from start_time) as month ,
	extract(year from start_time) as year,
	extract(dayofweek from start_time) as weekday
from
	staging_events se
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
