import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = 's3a://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json'
    song_data =  input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    print('song data reading start........')
    df = spark.read.json(song_data)
    print('song data reading complete........')
    
    # extract columns to create songs table
    songs_table = (df.select(['song_id','title','artist_id','year','duration']).distinct())
    
    # write songs table to parquet files partitioned by year and artist
    song_path = os.path.join(output_data,'songs')
    songs_table.write.parquet(song_path, mode='overwrite', partitionBy=['artist_id','year'])
    print('songs table created')
    
    # extract columns to create artists table
    artists_fields = ['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 
                      'artist_longitude as longitude']
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    # write artists table to parquet files
    artist_path = os.path.join(output_data,'artists')
    artists_table.write.parquet(artist_path, mode='overwrite')
    print('artists table created')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print('log data reading start........')
    #log_data = 's3a://udacity-dend/log-data/2018/11/2018-11-01-events.json'
    log_data = input_data + 'log_data/*/*/*.json'
    print('log data reading complete........')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).distinct()
    
    # write users table to parquet files
    users_path = os.path.join(output_data,'users')
    users_table.write.parquet(users_path, mode='overwrite')
    print('users table created')
  
    # create datetime column from original timestamp column
    df = df.withColumn("log_datetime",F.to_timestamp(F.from_unixtime((df.ts / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')))

    # extract columns to create time table
    time_table =  df.selectExpr(['log_datetime as start_time', 'hour(log_datetime) as hour', 'dayofmonth(log_datetime) as day',
                                 'weekofyear(log_datetime) as week','month(log_datetime) as month','year(log_datetime) as year',                                                        'dayofweek(log_datetime) as weekday']).distinct()
 
    # write time table to parquet files partitioned by year and month
    time_path = os.path.join(output_data,'time')
    time_table.write.parquet(time_path, mode='overwrite')
    print('time table created')

    # read in song data to use for songplays table
    song_path = os.path.join(output_data,'songs')
    song_df = spark.read.parquet(song_path)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  df.withColumn('songplay_id', F.monotonically_increasing_id()).join(song_df, song_df.title == df.song, 'left_outer') \
                       .select(['songplay_id', col("log_datetime").alias("start_time"), 'userId', 'level', 
                                'song_id', 'artist_id', 'sessionId', 'location', 'year']).distinct()

    # write songplays table to parquet files partitioned by year and month
    songplays_path = os.path.join(output_data,'songplays')
    songplays_table.write.parquet(songplays_path, mode='overwrite', partitionBy=['year'])
    print('songplays table created')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project-datalake-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
