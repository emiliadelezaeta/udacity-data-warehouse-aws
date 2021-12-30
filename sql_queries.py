import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

'''the following 2 staging tables will be used used as landing zone and all data types are VARCHAR to avoid conflicts 
copying data from S3, in the ETL the data types will be mapped correctly '''
'''but the column TS will be mapped as TIMESTAMP because in the COPY to redshift it can be transformed to epochmillisecs'''
'''documentation COPY --> https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat '''

staging_events_table_create = (""" CREATE TABLE IF NOT EXISTS staging_events (
                                    artist VARCHAR,
                                    auth VARCHAR,
                                    firstName VARCHAR,
                                    gender VARCHAR,
                                    itemInSession VARCHAR,
                                    lastName VARCHAR,
                                    length VARCHAR,
                                    level VARCHAR,
                                    location VARCHAR,
                                    method VARCHAR,
                                    page VARCHAR,
                                    registration VARCHAR,
                                    sessionId VARCHAR,
                                    song VARCHAR,
                                    status VARCHAR,
                                    ts TIMESTAMP,
                                    userAgent VARCHAR,
                                    userId VARCHAR
                                );

""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
                                    num_songs VARCHAR,
                                    artist_id VARCHAR,
                                    artist_latitude VARCHAR,
                                    artist_longitude VARCHAR,
                                    artist_location VARCHAR(max),
                                    artist_name VARCHAR(max),
                                    song_id VARCHAR,
                                    title VARCHAR,
                                    duration VARCHAR,
                                    year VARCHAR
                            );
""")

'''Maybe this fact table could be big, it's better to use a distkey to distribute data to each node and improve joins 
on this table '''
'''There is 2 options to chose a distkey, start_time or user_id that are coming from dimensions tables and NOT NULL'''
'''In this case, fact table and dimension table must use the same distribution key'''

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
                               songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                               start_time TIMESTAMP NOT NULL sortkey,
                               user_id INT NOT NULL distkey,
                               level VARCHAR,
                               song_id VARCHAR,
                               artist_id VARCHAR,
                               session_id INT,
                               location VARCHAR,
                               user_agent VARCHAR
                            );
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                                 user_id INT PRIMARY KEY distkey, 
                                 first_name VARCHAR NOT NULL, 
                                 last_name VARCHAR NOT NULL,
                                 gender VARCHAR, 
                                 level VARCHAR
                             ); 
""")

'''the following dimension tables will use distribution style ALL to replicate the table to each node to speed up 
joins '''

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(
                                    song_id VARCHAR PRIMARY KEY sortkey, 
                                    title VARCHAR NOT NULL, 
                                    artist_id VARCHAR, 
                                    year INT, 
                                    duration DECIMAL 
                                ) diststyle ALL;
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(
                                artist_id VARCHAR PRIMARY KEY sortkey,
                                name VARCHAR, 
                                location VARCHAR, 
                                latitude FLOAT, 
                                longitude FLOAT
                            ) diststyle ALL;
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(
                                start_time TIMESTAMP PRIMARY KEY sortkey, 
                                hour INT, 
                                day INT, 
                                week INT, 
                                month INT, 
                                year INT, 
                                weekday INT
                            ) diststyle ALL;
""")

# STAGING TABLES

'''documentation COPY JSON --> https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html '''

staging_events_copy = (""" COPY staging_events 
                           FROM {}
                           CREDENTIALS 'aws_iam_role={}'
                           JSON {}
                           TIMEFORMAT as 'epochmillisecs'
                           REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = (""" COPY staging_songs 
                          FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          JSON 'auto'  
                          REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

''' Some columns are formatted (CAST) before insert them into the final table'''

songplay_table_insert = (""" 
          INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location,user_agent)
            SELECT  ts,
                    CAST(events.userId as integer),
                    events.level,
                    songs.song_id,
                    songs.artist_id,
                    CAST(events.sessionId as integer),
                    events.location,
                    events.userAgent
            FROM staging_events events 
            INNER JOIN staging_songs songs ON (events.artist = songs.artist_name AND events.song = songs.title)
            WHERE events.page = 'NextSong' 
    """)

user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
            SELECT DISTINCT CAST(userId as integer),
                    firstName,
                    lastName, 
                    gender, 
                    level
            FROM staging_events
            WHERE page = 'NextSong'
""")

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
            SELECT DISTINCT song_id,
                title, 
                artist_id, 
                CAST(year as integer),
                CAST(duration as decimal)
            FROM staging_songs
""")

artist_table_insert = (""" 
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
            SELECT DISTINCT artist_id, 
                artist_name,
                artist_location,
                CAST(artist_latitude as float), 
                CAST(artist_longitude as float)
            FROM staging_songs
""")

time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
            SELECT ts, 
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
            FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
