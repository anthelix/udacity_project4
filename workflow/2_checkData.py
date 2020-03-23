#!/usr/bin/python3

# SONGS_TABLE

# check Null for song_id in set df_song
df_sorted = df_song.cube('song_id').count() \
    .filter('song_id NOT LIKE \'NONE\'') \
    .filter('song_id NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'song_id'], ascending=[False, True])
df_sorted.show(2, False)
print('Not Null with filter: %d '% df_song.filter('song_id != ""').count())
print('Null with filter: %d '% df_song.filter('song_id == ""').count())

# check Null for title in set df_song
df_sorted = df_song.cube('title').count() \
    .filter('title NOT LIKE \'NONE\'') \
    .filter('title NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'title'], ascending=[False, True])
df_sorted.show(2, False)
print('Not Null with filter: %d '% df_song.filter('title != ""').count())
print('Null with filter: %d '% df_song.filter('title == ""').count())

# check Null for artist_id in set df_song
df_sorted = df_song.cube('artist_id').count() \
    .filter('artist_id NOT LIKE \'NONE\'') \
    .filter('artist_id NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'artist_id'], ascending=[False, True])
df_sorted.show(2, False)
print('Not Null with filter: %d '% df_song.filter('artist_id != ""').count())
print('Null with filter: %d '% df_song.filter('artist_id == ""').count())

print('Numbers of rows in songs_table : %d' % songs_table.count())
print('Not Null song_id with filter: %d '% songs_table.where(col("song_id").isNotNull()).count())
print('Null with song_id filter: %d '% songs_table.where(col("song_id").isNull()).count())
print('Not Null title with filter: %d '% songs_table.where(col("title").isNotNull()).count())
print('Null with title filter: %d '% songs_table.where(col("title").isNull()).count())
print('Not Null artist_id with filter: %d '% songs_table.where(col("artist_id").isNotNull()).count())
print('Null with artist_id filter: %d '% songs_table.where(col("artist_id").isNull()).count())
check_parquet(parquet_path)



# ARTISTS_TABLE

# check Null for artist_id in set df_song
df_sorted = df_song.cube('artist_id').count() \
    .filter('artist_id NOT LIKE \'NONE\'') \
    .filter('artist_id NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'artist_id'], ascending=[False, True])
df_sorted.show(2, False)
print('Not Null artist_id with filter: %d '% df_song.filter('artist_id != ""').count())
print('Null artist_id with filter: %d '% df_song.filter('artist_id == ""').count())

# check Null for title in set df_song
df_sorted = df_song.cube('artist_name').count() \
    .filter('artist_name NOT LIKE \'NONE\'') \
    .filter('artist_name NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'artist_name'], ascending=[False, True])
df_sorted.show(2, False)
print('Not Null artist_name with filter: %d '% df_song.filter('artist_name != ""').count())
print('Null artist_name with filter: %d '% df_song.filter('artist_name == ""').count())

print('Numbers of rows in artists_table : %d' % artists_table.count())
print('Not Null artist_id  with filter: %d '% artists_table.where(col("artist_id").isNotNull()).count())
print('Null with artist_id  filter: %d '% artists_table.where(col("artist_id").isNull()).count())
print('Not Null name with filter: %d '% artists_table.where(col("name").isNotNull()).count())
print('Null with name filter: %d '% artists_table.where(col("name").isNull()).count())
check_parquet(parquet_path)



# USERS_TABLE

# check Null for userId in set log_clean
df_sorted = df_log.cube('userId').count() \
    .filter('userId NOT LIKE \'NONE\'') \
    .filter('userId NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'userId'], ascending=[False, True])
df_sorted.show(5, False)
print('Not Null userId with filter: %d '% df_log.filter('userId != ""').count())
print('Null userId with filter: %d '% df_log.filter('userId == ""').count())

# check Null for level in set log_raw
df_sorted = df_log.cube('level').count() \
    .filter('level NOT LIKE \'NONE\'') \
    .filter('level NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'level'], ascending=[False, True])
df_sorted.show(5, False)
print('Not Null level with filter: %d '% df_log.filter('level != ""').count())
print('Null level with filter: %d '% df_log.filter('level == ""').count())

print('Numbers of rows in users_table : %d' % users_table.count())
print('Not Null user_idwith filter: %d '% users_table.where(col("user_id").isNotNull()).count())
print('Null with user_id filter: %d '% users_table.where(col("user_id").isNull()).count())
check_parquet(parquet_path)



# TIME_TABLE

# check Null for userId in set log_clean
df_sorted = df_log.cube('formated_ts').count() \
    .filter('formated_ts NOT LIKE \'NONE\'') \
    .filter('formated_ts NOT LIKE \'Adjustment\'') \
    .orderBy(['count', 'formated_ts'], ascending=[False, True])
df_sorted.show(5, False)
print('Not Null formated_ts with filter: %d '% df_log.where(col("formated_ts").isNotNull()).count())
print('Null with formated_ts filter: %d '% df_log.where(col("formated_ts").isNull()).count())

print('Numbers of rows in users_table : %d' % time_table.count())
print('Not Null with filter: %d '% time_table.where(col("start_time").isNotNull()).count())
print('Null with filter: %d '% time_table.where(col("start_time").isNull()).count())


# SONGPLAYS_TABLE

print('Numbers of rows in songplays_table : %d' % songplays_table.count())
print('Not Null start_time with filter: %d '% songplays_table.where(col("start_time").isNotNull()).count())
print('Null start_time with filter: %d '% songplays_table.where(col("start_time").isNull()).count())
print('Not Null user_id with filter: %d '% songplays_table.where(col("user_id").isNotNull()).count())
print('Null user_id with filter: %d '% songplays_table.where(col("user_id").isNull()).count())
print('Not Null level with filter: %d '% songplays_table.where(col("level").isNotNull()).count())
print('Null level with filter: %d '% songplays_table.where(col("level").isNull()).count())
print('Not Null session_id with filter: %d '% songplays_table.where(col("session_id").isNotNull()).count())
print('Null session_id with filter: %d '% songplays_table.where(col("session_id").isNull()).count())
check_parquet(parquet_path)

