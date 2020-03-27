"""
SCRIPT ETL:  LOAD S3 AND WRITE IN PARQUET FILES
Choose in the main():
    input_data = "s3a://udacity-dend/"
    input_data = ""

    output_data = "./output/" to run locally
    output_data = "s3a://<YOUR BUCKET>" 
"""

from pyspark.sql.types import TimestampType, StructType, StructField, FloatType, IntegerType, LongType, StringType, DataType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf,col
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from datetime import datetime
import configparser
import pandas as pd
import os

def get_credentials():
    """
    get AWS keys
    """
    try:
        # parse file
        config = configparser.ConfigParser()
        config.read('dl.cfg')
        # set AWS variables
        os.environ['AWS_ACCESS_KEY_ID']     = config['AWS']['KEY']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']
    except Exception as e:
        print("Unexpected error: %s" % e)


def create_spark_session():
    """
    create or load a Spark session
    """
    try:
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        return spark
    except Exception as e:
        print("Unexpected error: %s" % e)

def write_parquet_song(table, parquet_path):
    """
    write parquet files partionned in year and artist_id
    """
    try:
        table.write.partitionBy("year", "artist_id").parquet(parquet_path, mode = 'overwrite')
    except Exception as e:
        print("Unexpected error: %s" % e)

def write_parquet(table, parquet_path):
    """
    write parquet files 
    """
    try:
        table.write.parquet(parquet_path, mode = 'overwrite')
    except Exception as e:
        print("Unexpected error: %s" % e)

def write_parquet_time(table, parquet_path):
    """
    write parquet files partionned in year and month
    """
    try:
        table.write.partitionBy(['year', 'month']).parquet(parquet_path, mode = 'overwrite')
    except Exception as e:
        print("Unexpected error: %s" % e)

def create_song_data(spark, input_data):
    """
    process song data
    """
    try:
        # get filepath to song data file
        song_data = "song_data/*/*/*/*.json"
        input_song = input_data + song_data

        # read song data file
        song_schema = StructType([
            StructField("num_songs", IntegerType()),
            StructField("artist_id", StringType()),
            StructField("artist_latitude", FloatType()),
            StructField("artist_longitude", FloatType()),
            StructField("artist_location", StringType()),
            StructField("artist_name", StringType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("duration", FloatType()),
            StructField("year", IntegerType())
        ])    
        df_song = spark.read.json(input_song, schema = song_schema)
        df_song.printSchema()
        # print('song_data rows: %d' % df_song.count())
        # print('song_data schema: %s' % df_song)
        return df_song
    except Exception as e:
        print("Unexpected error: %s" % e)

def create_log_data(spark, input_data):
    """
    process log data
    """
    try:
        # get filepath to log data file
        log_data ="log_data/*/*/*.json"
        input_log = input_data + log_data

        # read log data file
        log_schema = StructType([
            StructField("artist", StringType()),
            StructField("auth", StringType()),
            StructField("firstName", StringType()),
            StructField("gender", StringType()),
            StructField("itemInSession", IntegerType()),
            StructField("lastName", StringType()),
            StructField("length", FloatType()),    
            StructField("level", StringType()),
            StructField("location", StringType()),
            StructField("method", StringType()),
            StructField("page", StringType()),
            StructField("registration", FloatType()),
            StructField("sessionId", StringType()),
            StructField("song", StringType()),
            StructField("status", IntegerType()),
            StructField("ts", LongType()),
            StructField("userAgent", StringType()),
            StructField("userId", StringType())
        ])    
        df_log_raw = spark.read.json(input_log, schema = log_schema)
        # filter by actions for song plays
        df_log = df_log_raw.filter("page='NextSong'")   
        # print('log_data rows: %d' % df_log.count())
        df_log.printSchema()
        # print('log_data schema: %s' % df_log)
        return df_log
    except Exception as e:
        print("Unexpected error: %s" % e)

def process_song_data(df_song, output_data):
    """
    create songs_table and artists_table
    write tables to parquet files    
    """
    try:
        # extract columns to create songs table
        songs_table = df_song \
            .drop_duplicates(['song_id']) \
            .select("song_id", "title", "artist_id", "year", "duration") \
            .filter('song_id != "" and title != "" and artist_id != ""') \
            .sort("song_id")
        songs_table.show(2)

        # write songs table to parquet files partitioned by year and artist
        songs_table.collect()
        parquet_path = output_data + 'songs_table'
        write_parquet_song(songs_table, parquet_path)

        # extract columns to create artists table
        artists_table = df_song \
            .drop_duplicates(['artist_id']) \
            .selectExpr("artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude") \
            .filter('artist_id != "" and name != ""') \
            .sort("artist_id")
        artists_table.show(2)
        
        # write artists table to parquet files
        artists_table.collect()
        parquet_path = output_data + 'artists_table'
        write_parquet(artists_table, parquet_path)
        # print('songs_table rows: %d' % songs_table.count())
        # print('artist_table rows: %d' % artists_table.count())
        return(songs_table, artists_table)
    except Exception as e:
        print("Unexpected error: %s" % e)


def process_log_data(df_log, df_song, output_data):
    """
    create time_table, users_table  and songplays_table
    write tables to parquet files
    """
    try:
        # extract columns for users table    
        users_table = df_log \
            .drop_duplicates(subset = ['userId']) \
            .filter('level != ""' and 'userId != ""') \
            .orderBy("ts", ascending = False) \
            .coalesce(1)\
            .selectExpr("cast(userId as Long) user_id", "firstName as first_name", "lastName as last_name", "gender", "level")   \
            .sort('user_id')
        users_table.show(2)
        
        # write users table to parquet files
        users_table.collect()
        parquet_path = output_data + 'users_table'
        write_parquet(users_table, parquet_path)

        # create timestamp column from original timestamp column
        get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x/1000), T.TimestampType())
        # remove rows with empty ts value
        df_copy = df_log.dropna(subset='ts')  
        # add a new column 'timestamp' 
        df_log_formated = df_copy.withColumn("timestamp", get_timestamp(df_copy.ts))
    
        # extract columns to create time table
        time_df = df_log_formated \
            .drop_duplicates(['timestamp']) \
            .select( \
                col('timestamp').alias("start_time"),
                hour(col('timestamp')).alias('hour'),
                dayofmonth(col('timestamp')).alias('day'),
                weekofyear(col('timestamp')).alias('week'),
                month(col('timestamp')).alias('month'),
                year(col('timestamp')).alias('year')) \
            .sort('start_time')

        time_table = time_df.withColumn('hour', F.hour('start_time')) \
                        .withColumn('day', F.dayofmonth('start_time')) \
                        .withColumn('year', F.year('start_time')) \
                        .withColumn('week', F.weekofyear('start_time')) \
                        .withColumn('month', F.month('start_time')) \
                        .withColumn('weekday', F.dayofweek('start_time').cast("string"))

        time_table.show(20)
        
        # write time table to parquet files partitioned by year and month
        time_table.collect()
        parquet_path = output_data + 'time_table'
        write_parquet_time(time_table, parquet_path)

        # extract columns from joined song and log datasets to create songplays table 
        tl = df_log_formated.alias('tl')
        ts = df_song.alias('ts')    
        inner_join = tl.join(ts, ((tl.artist == ts.artist_name) & (tl.artist == ts.artist_name)), how='inner')    
        songplays = inner_join \
                .withColumn("songplay_id", monotonically_increasing_id()) \
                .filter('timestamp != ""' and 'userId != ""' and 'level != ""' and 'sessionId != ""')
        
        songplays_table = songplays \
                        .selectExpr("songplay_id",
                                        "timestamp as start_time",
                                        "cast(userId as Long) user_id",
                                        "level",
                                        "song_id",
                                        "artist_id",
                                        "sessionId as session_id",
                                        "location",
                                        "userAgent as user_agent") \
                        .sort('songplay_id') \
                        .withColumn('year', F.year('start_time')) \
                        .withColumn('month', F.month('start_time')) 
        songplays_table.show(5)

        # write songplays table to parquet files partitioned by year and month
        songplays_table.collect()
        parquet_path = output_data + 'songplays_table'
        write_parquet_time(songplays_table, parquet_path)
        # print('users_table rows: %d' % users_table.count())
        # print('time_table rows: %d' % time_table.count())
        # print('songplays rows: %d' % songplays_table.count())
        return(users_table, time_table, songplays_table)
    except Exception as e:
        print("Unexpected error: %s" % e)

def main():
    input_data = "s3a://udacity-dend/"              # Data from Udacity
    # input_data = ""                               # Run locally

    # output_data = "s3a://dend-paris/sparkify/"    # Anthelix bucket(me)
    # output-data = "s3a://<YOUR_BUCKET>/"          # Visitor bucket
    output_data = "./output/"                       # Run localy

    print("\n")
    print("...Get AWS keys...")
    get_credentials()
    print("\n")
    print("...Create a Spark session...")
    spark = create_spark_session()
    print("\n")
    print("...Create dataframe for Song Data...")
    df_song = create_song_data(spark, input_data)
    print("\n")
    print("...Create dataframe for Log Data...")
    df_log = create_log_data(spark, input_data)
    print("\n")
    print("...Process song data...")
    songs_table, artists_table = process_song_data(df_song, output_data)
    print("\n")
    print("...Process log data...")
    process_log_data(df_log, df_song, output_data)

if __name__ == "__main__":
    main()
