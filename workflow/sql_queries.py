
# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS factSongplay"
user_table_drop = "DROP TABLE IF EXISTS Dimuser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

## Dimension Tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUser
(
    user_id bigint NOT NULL PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar(1),
    level varchar NOT NULL
    )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSong
(
    song_id varchar NOT NULL PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int,
    duration numeric
    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtist
(
    artist_id varchar NOT NULL PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude numeric,
    longitude numeric
    )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime
(
    start_time timestamp NOT NULL PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar
    )
""")

## Fact Tables
sequence = ("""
    CREATE SEQUENCE IF NOT EXISTS transactions_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 ;
""")
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factSongplay
(
    songplay_id  serial NOT NULL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id bigint NOT NULL ,
    level varchar NOT NULL,
    song_id varchar,
    artist_id varchar,
    session_id varchar NOT NULL,
    location varchar,
    user_agent varchar
    )
""")
# QUERY LISTS

create_table_queries = [sequence, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
