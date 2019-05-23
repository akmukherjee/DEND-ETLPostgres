""" 
    This file contains the functions to create  and drop the songplays, users, songs, artists and time tables.
    Addtionally, the create and drop queries are added to a list to be interatively executed.
"""
# DROP TABLES
"""
SQL String to drop the individual tables are listed below
"""

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (songplay_id SERIAL , start_time TIMESTAMP NOT NULL,userId VARCHAR(255) NOT NULL,level VARCHAR(8),song_id VARCHAR(255), artist_id VARCHAR(255),session_id INTEGER, location VARCHAR(255),user_agent TEXT);
""")

user_table_create = ("""
CREATE TABLE users (userId VARCHAR(255) PRIMARY KEY, firstName VARCHAR(255), lastName VARCHAR(255),gender CHAR(1),level VARCHAR(8));
""")

song_table_create = ("""
CREATE TABLE songs (song_id VARCHAR(255) PRIMARY KEY, title VARCHAR(255), artist_id VARCHAR(255), year INTEGER, duration NUMERIC);
""")

artist_table_create = ("""
CREATE TABLE artists(artist_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255),location VARCHAR(255),lattitude NUMERIC, longitude NUMERIC);
""")

time_table_create = ("""
CREATE TABLE time (start_time TIMESTAMP PRIMARY KEY, hour INTEGER, day INTEGER, week INTEGER, month INTEGER, year INTEGER, weekday VARCHAR(20));
""")



songplay_table_insert = ("""
INSERT into songplays (start_time,userId,level,song_id,artist_id,session_id,location,user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
INSERT into users (userId,firstName,lastName,gender,level) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (userId) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT into songs (song_id,artist_id,year,duration) VALUES(%s,%s,%s,%s)
""")


artist_table_insert = ("""
INSERT into artists (artist_id,name,location,lattitude,longitude) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
""")


time_table_insert = ("""
INSERT into time (start_time,hour,day,week,month,year,weekday) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id 
FROM songs 
INNER JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title =%s AND artists.name =%s AND songs.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]