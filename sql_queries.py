import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events; "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs; "
songplay_table_drop = "DROP TABLE IF EXISTS songplay; "
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" 
CREATE TABLE IF NOT EXISTS staging_events_table(
                            artist TEXT,
                            auth TEXT,
                            first_name TEXT,
                            gender CHAR(1),
                            item_session TEXT,
                            last_name TEXt,
                            length NUMERIC,
                            level TEXT,
                            location TEXT,
                            method TEXT,
                            page TEXT,
                            registration NUMERIC,
                            session_id INTEGER,
                            song TEXT,
                            status INTEGER,
                            ts BIGINT,
                            user_agent TEXT,
                            user_id Integer)

""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_song_table(
                            num_songs INTEGER,
                            artist_id TEXT,
                            artist_latitude NUMERIC,
                            artist_longitude NUMERIC,
                            artist_location TEXT,
                            artist_name TEXT,
                            song_id TEXT,
                            title TEXT,
                            duration NUMERIC,
                            year INTEGER)
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay_table(
                            songplay_id INT IDENTITY(1,1) PRIMARY KEY,
                            start_time TIMESTAMP,
                            user_id INTEGER NOT NULL,
                            level TEXT,
                            song_id TEXT,
                            artist_id TEXT,
                            session_id INTEGER,
                            location TEXT,
                            user_agent TEXT)
""")

user_table_create = (""" CREATE TALBE IF NOT EXISTS user_table( 
                            user_id INTEGER PRIMARY KEY,
                            first_name TEXT,
                            last_name TEXT,
                            gender CHAR(1),
                            level TEXT)
""")

song_table_create = (""" CREATE TALBE IF NOT EXISTS song_table(
                            song_id TEXT PRIMARY KEY,
                            title TEXT,
                            artist_id TEXT,
                            year INTEGER,
                            duration NUMERIC)
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist_table(
                            artist_id TEXT PRIMARY KEY,
                            name TEXT,
                            location TEXT,
                            latitude NUMERIC,
                            longitude NUMERIC)
""")

time_table_create = (""" CREATE TALBE IF NOT EXISTS time_table(
                            start_time TIMESTAMP PRIMARY KEY,
                            hour INTEGER,
                            day INTEGER,
                            week INTEGER,
                            month INTEGER,
                            year INTEGER,
                            weekDay INTEGER)
""")

# STAGING TABLES TO COPY S3 FILES TO STAGING TALBES

staging_events_copy = (""" COPY staging_events_table
                           FROM {}
                           credentials 'aws_iam_role='
                           JSON{};
""").format(LOG_DATA,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs_table
                         FROM {}
                         credentials 'aws_iam_role='
                         json 'auto';
""").format(SONG_DATA)

# FINAL TABLES TO INSER RECORDS FROM STAGING TABLES TO FACT AND DIMENTION TABLES

songplay_table_insert = (""" INSERT INTO songplay_table(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                             SELECT timestamp 'epoch' + SE.ts/1000 * interval '1 second' AS start_time, 
                             SE.user_id,
                             SE.level,
                             SS.song_id,
                             SS.ssesion_id,
                             SE.location,
                             SE.user_agent
                             FROM staging_events_table AS SE   
                             LEFT JOIN staging_songs_table SS
                            ON SE.artist = SS.artist_name
                            AND SE.song = SS.title
                            WHERE SE.page = 'NextSong';
""")

user_table_insert = (""" INSER INTO user_table(user_id,first_name, last_name, gender, level)
                         SELECT DISTINCT user_id, first_name, last_name, gender, level
                         FROM staging_events_table
                         WHERE page = 'NextSong';
""")

song_table_insert = (""" INSER INTO song_table (song_id, title, artist_id, year, duration)
                         SELECT song_id, title, artist_id, year, duration
                         FROM staging_songs_table
                         WHERE song_is IS NOT NULL;
""")

artist_table_insert = (""" INSER INTO artist_table (artist_id, name, location, altitude, longitude)
                            SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            FROM staging_songs_table
                            WHERE artist_id IS NOT NULL;
""")

time_table_insert = (""" INSERT INTO time_table (start_time, hour, day, week, month, year, weekDay)
                         SELECT start_time, 
                         EXTRACT (hour from start_time) AS hour,
                         EXTRACT (day from start_time) AS day, 
                         EXTRACT (week from start_time) AS week,
                         EXTRAXT (month from start_time) AS month,
                         EXTRACT (year from start_time) AS year,
                         EXTRACT (dayofweek from start_time) AS weekDay
                         FROM songpaly_table;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
