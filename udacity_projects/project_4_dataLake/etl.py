import configparser
from datetime import datetime
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # get filepath to song data file in the S3 Bucket
    song_data = input_data + "song_data/A/A/*/*.json"
    
    # defining the schema for the song_table
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])

    
    # geting song data files from S3
    print("Load song_data starting...")
    start = time.time()
    df = spark.read.json(song_data, schema=song_schema)
    end = time.time()
    print("...Load song_data completed. Time taken for fun program: ", end - start)
    df.printSchema()

    
    # extract columns to create songs table
    print('Loading songs_table starts...')
    start = time.time()
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")
    end = time.time()
    print("...Loading songs_table completes. Time taken for fun program: ", end - start)

    
    # extract columns to create artists table
    print('Loading artists_table starts...')
    start = time.time()
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude",
                      "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
  

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')
    end = time.time()
    print("...Loading artists_table completes. Time taken for fun program: ", end - start)

    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file from S3
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    
    # read log data file
    print("Load log_data starting...")
    df_log = spark.read.json(log_data)
    print("...Load log_data completed.")
 

    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    
    # extract columns for users table 
    print('Load users_table starts...')
    start = time.time()
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df_log.selectExpr(users_fields).dropDuplicates()
 

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')
    end = time.time()
    print("...Loading users_table completes. Time taken for fun program: ", end - start)

    
    # create timestamp column from original timestamp column
    
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df_log = df_log.withColumn("start_time", get_datetime(df_log.timestamp))

    df_log = df_log.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))
                    
    
    # extract columns to create time table
    print('Loading time_table starts...')
    start = time.time()
    time_table = df_log.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")
    
    end = time.time()
    print("...Loading time_table completes. Time taken for fun program: ", end - start)

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))

    # read in song data to use for songplays table by joining the dataframe
    songplays_table = df_log.join(song_df, df_log.song == song_df.title, how='inner')\
        .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),\
                "level","song_id","artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", \
                "location", "user_agent", "year", "month")


    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), \
                      mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data  = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
