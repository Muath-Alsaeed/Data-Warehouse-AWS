import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays; "
user_table_drop = " DROP TABLE IF EXISTS users ; "
song_table_drop = "DROP TABLE IF EXISTS  songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = " DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""

CREATE TABLE staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  bigint,
        userAgent           VARCHAR,
        userId              INTEGER 
    );

""")

staging_songs_table_create = ("""
 CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
""")

songplay_table_create = ("""

CREATE TABLE songplays(songplay_id int IDENTITY(0,1) PRIMARY KEY,
                        start_time timestamp NOT NULL,
                        userId int NOT NULL,
                        level  varchar, 
                        song_id varchar NOT NULL ,
                        artist_id varchar NOT NULL, 
                        sessionId int,location text , 
                        userAgent varchar
);

""")

user_table_create = ("""

 CREATE TABLE users(userId int PRIMARY KEY,
                    firstName varchar, 
                    lastName varchar,
                    gender varchar, 
                    level  varchar
);
""")

song_table_create = ("""
CREATE TABLE songs(song_id varchar PRIMARY KEY,
                   title varchar, 
                   artist_id varchar,
                   year int, 
                   duration double precision
);
""")

artist_table_create = ("""
CREATE TABLE artists(artist_id varchar PRIMARY KEY,
                    artist_name varchar, 
                    artist_location varchar,
                    artist_latitude double precision, 
                    artist_longitude double precision
);
""")

time_table_create = ("""
CREATE TABLE time (start_time timestamp  PRIMARY KEY,
                 hour int, day int, week int,
                 month int,year int, weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""

copy staging_events from {data_bucket} 
credentials 'aws_iam_role={role_arn}' 
region 'us-west-2' 
format as JSON {LOG_JSON_PATH} 
timeformat as 'epochmillisecs';
""").format(data_bucket = config['S3'] ['LOG_DATA'] ,
            role_arn = config['IAM_ROLE']['ARN'],
            LOG_JSON_PATH = config['S3']['LOG_JSONPATH'] )

staging_songs_copy = ("""


copy staging_songs from {data_bucket} 
credentials 'aws_iam_role={role_arn}' 
region 'us-west-2' 
format as JSON 'auto'; 
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays ( start_time, userId, level,song_id, artist_id,sessionId,location, userAgent)

SELECT DISTINCT 
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        E.userId,
        E.level,
        S.song_id,
        S.artist_id,
        E.sessionId,
        E.location,
        E.userAgent
from staging_events E
INNER JOIN  staging_songs S 
on(E.artist = S.artist_name)
AND E.page = 'NextSong';
        
        
        
""")

user_table_insert = ("""
insert into users (  userId , firstName , lastName ,gender, level)

SELECT DISTINCT  userId,

  
        firstName,          
        gender ,
       
        lastName,
         level 
         FROM staging_events
         WHERE userId IS NOT NULL 
         AND  page = 'NextSong';
       

""")

song_table_insert = ("""
insert into songs (  song_id , title , 
artist_id , year , duration )

SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,           
        duration            
FROM staging_songs
WHERE song_id IS NOT NULL;

""")

artist_table_insert = ("""
INSERT into artists (  artist_id , artist_name , 
artist_location ,artist_latitude, artist_longitude)
SELECT DISTINCT artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs;

""")

time_table_insert = ("""
insert into time (  start_time, hour, day, week, month, year, weekday) 
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
EXTRACT(hour FROM start_time) AS hour ,
EXTRACT (day FROM start_time) AS  day,
EXTRACT (week FROM start_time) AS  week,
EXTRACT (month FROM start_time ) AS month,
EXTRACT (year FROM start_time ) AS year,
EXTRACT (weekday FROM start_time ) AS weekday

FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
