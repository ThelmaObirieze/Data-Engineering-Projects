# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
  
songplay_table_create = ("CREATE TABLE songplays (songplay_id text PRIMARY KEY, start_time text NOT NULL, user_id text NOT NULL, \
                         level text, song_id text, artist_id text, session_id text, location text, user_agent text) ")

user_table_create = ("CREATE TABLE users (user_id text PRIMARY KEY, first_name text, last_name text, gender text, level text)")

song_table_create = ("CREATE TABLE songs (song_id text PRIMARY KEY, title text NOT NULL, artist_id text NOT NULL, year int, duration real)")

artist_table_create = ("CREATE TABLE  artists (artist_id text PRIMARY KEY, name text NOT NULL, location text, latitude real, longitude real)")

time_table_create = ("CREATE TABLE  time (start_time text PRIMARY KEY, hour text, day text, week text, month text, year text, weekday text)")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (song_id, start_time,user_id, level, artist_id, session_id, location, user_agent)  
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level ) 
                        VALUES (%s, %s, %s, %s, %s) 
                        on conflict(user_id) do update set level=excluded.level""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                         VALUES (%s, %s, %s, %s, %s) 
                         ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                            VALUES (%s, %s, %s, %s, %s) 
                            ON CONFLICT (artist_id) DO NOTHING""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s) 
                            ON CONFLICT (start_time) DO NOTHING """)

# FIND SONGS

song_select = ("""SELECT song_id, s.artist_id
FROM artists AS a
JOIN songs AS s
ON a.artist_id = s.artist_id
WHERE s.title = (%s)
AND a.name = (%s)
AND s.duration = (%s);
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]