import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        DESCRIPTION: This function loads the songs datasets from amazon s3 bucket into a staging dataframe,
        creates the songs and artists tables and load them back to s3 as the final tables.
        
        PARAMETERS:
            spark       : SparkSession
            input_data  : input data path for process
            output_data : output data path to write back results
    """
    
    # get file path to the song datasets
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # define song schema
    songSchema = StructType([
        SructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())
        
    ])
    
    
    # read song data file
    song_df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    song_col = ["song_id","title", "artist_id","year", "duration"]
    songs_table = song_df.select(song_col).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs")

    # extract columns to create artists table
    artists_col = ["artist_id", 
                   "artist_name", 
                   "artist_location as location", 
                   "artist_latitude as latitude", 
                   "artist_longitude as longitude"]
    
    artists_table = song_df.selectExpr(artists_columns).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists")


                                
def process_log_data(spark, input_data, output_data):
    """
        DESCRIPTION: This function loads the events log datasets from amazon s3 bucket into a staging dataframe,
        creates the users, time and songplays tables and load them back to s3 as the final tables.
        
        PARAMETERS:
            spark       : SparkSession
            input_data  : input data path for process
            output_data : output data path to write back results
    """                          
                                
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
                                
    # define log data schema
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
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", StringType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ])
                                

    # read log data file
    event_log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    event_log_df = event_log_df.filter(df.page == "NextSong")

    # extract columns for users table
    users_col = ["userdId as user_id", 
                 "firstName as first_name", 
                 "lastName as last_name", 
                 "gender", 
                 "level"]
    users_table = event_log_df.selectExpr(users_col).dropDuplicates()
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    event_log_df = event_log_df.withColumn("timestamp", get_timestamp(event_log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    event_log_df = event_log_df.withColumn("start_time", get_datetime(event_log_df.ts))
    
    # extract columns to create time table
    event_log_df = event_log_df.withColumn("hour", hour("start_time")) \
                            .withColumn("day", dayofmonth("start_time")) \
                            .withColumn("week", weekofyear("start_time")) \
                            .withColumn("month", month("start_time")) \
                            .withColumn("year", year("start_time")) \
                            .withColumn("weekday", dayofweek("start_time"))
    
    time_table = event_log_df.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time"))

                                
    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
                                
    cond = [event_log_df.artist == song_df.artist_name, event_log_df.song == song_df.title]
    song_log_df = event_log_df.join(song_df, cond, 'outer')

                                
    # extract columns from joined song and log datasets to create songplays table                               
    songplays_table = song_log_df.select(
        col("start_time").alias("start_time"),
        col("userId").alias("user_id"),
        col("level").alias("level"),
        col("song_id").alias("song_id"),
        col("artist_id").alias("artist_id"),
        col("sessionId").alias("session_id"),
        col("location").alias("location"),
        col("userAgent").alias("user_agent"),
        col("year").alias("year"))\
    .withColumn("month", month(col("start_time"))).dropDuplicates()
    
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays.write.partitionBy('year', 'month').parquet(output_data + 'songplays')


def main():
    """
        The main part of the function creates the spark session
        defined the input and output paths
        processes the input data to create all relevant data for the
        star dimentional schema model for sparkify and writes the tables
        back to s3 bucket.
    """
    spark = create_spark_session()
    input_song_data = "s3://udacity-dend/song_data/A/A/A/"
    input_log_data = "s3://udacity-dend/log_data/2018/11/"
    output_data = "s3://aws-logs-377518875856-us-west-2/elasticmapreduce/"
    
    process_song_data(spark, input_song_data, output_data)    
    process_log_data(spark, input_log_data, output_data)


if __name__ == "__main__":
    main()
