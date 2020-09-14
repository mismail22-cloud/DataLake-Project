#import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql import types as T
from datetime import datetime

#Comment the Config as it is not needed when running directly on EMR
#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

#This function will check if there exists an earlier load for the table or this is the first time
def check_table(table_path):
    try:
        song_table_df = spark.read.parquet(table_path).select('song_id', 'title', 'artist_id', 'year', 'duration')
        return 'TRUE'
    except:
        return 'FALSE'

#This function transform to timestamp 
def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)

def create_spark_session():
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    print('Loading Songs Files')
    # get filepath to song data file
    song_delta_path = '{}song_data//*//*//*//*.json'.format(input_data)
    
    # read song data file
    song_schema = StructType([
        StructField('num_songs', IntegerType()),
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', DoubleType()),
        StructField('year', IntegerType())
    ])
    
    
    song_file_df = spark.read.json(song_delta_path,schema=song_schema)
    
    
    print('Load Song Table')
    # extract columns to create songs table
    song_table_path = '{}song.parquet'.format(output_data)
    song_delta_df =song_file_df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    tab_exist_flg=check_table(song_table_path)
    
    # write songs table to parquet files partitioned by year and artist
    #check if the parquet file is loaded before or not
    #if loaded before then make left join to increment on the existing data else load the full delta
    if tab_exist_flg=='TRUE':
        song_table_df = spark.read.json(song_delta_path).select('song_id', 'title', 'artist_id', 'year', 'duration')
        song_join_df=song_delta_df.join(song_table_df, song_delta_df.song_id == song_table_df.song_id , 'left_outer').filter(song_table_df.song_id.isNull())
        song_insrt_df=song_join_df.select(song_delta_df.song_id, song_delta_df.title, song_delta_df.artist_id, song_delta_df.year, song_delta_df.duration)
        song_insrt_df.write.partitionBy('year','artist_id').parquet(song_table_path, 'append')
    else:
        song_insrt_df=song_delta_df.select(song_delta_df.song_id, song_delta_df.title, song_delta_df.artist_id, song_delta_df.year, song_delta_df.duration)
        song_insrt_df.write.partitionBy('year','artist_id').parquet(song_table_path, 'append')
    
    
    print('Load Artist Table')
    # extract columns to create artists table
    artist_table_path = '{}artist.parquet'.format(output_data)
    artist_delta_df =song_file_df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as lattitude', \
                                             'artist_longitude as longitude').dropDuplicates()
    # write artists table to parquet files
    tab_exist_flg=check_table(artist_table_path)
    
    #check if the parquet file is loaded before or not
    #if loaded before then make left join to increment on the existing data else load the full delta
    if tab_exist_flg=='TRUE':
        artist_table_df = spark.read.json(artist_delta_path).select('artist_id', 'name', 'location', 'lattitude', 'longitude')
        artist_join_df=artist_delta_df.join(artist_table_df, artist_delta_df.artist_id == artist_table_df.artist_id , 'left_outer').filter(artist_table_df.artist_id.isNull())
        artist_insrt_df=artist_join_df.select(artist_delta_df.artist_id, artist_delta_df.name, artist_delta_df.location, artist_delta_df.lattitude, artist_delta_df.longitude)
        artist_insrt_df.write.parquet(artist_table_path, 'append')
    else:
        artist_insrt_df=artist_delta_df.select(artist_delta_df.artist_id, artist_delta_df.name, artist_delta_df.location, \
                                               artist_delta_df.lattitude, artist_delta_df.longitude)
        artist_insrt_df.write.parquet(artist_table_path, 'append')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print('Load Log file')
    

    # read log data file
    log_delta_path = '{}log_data//*//*//*.json'.format(input_data)
    log_file_df_all = spark.read.json(log_delta_path)
    # filter by actions for song plays
    log_file_df=log_file_df_all.filter(log_file_df_all.page=="NextSong")
 

    # extract columns for users table   
    print('Load Users Table')
    user_table_path = '{}user.parquet'.format(output_data)
    user_delta_df =log_file_df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    #check if the parquet file is loaded before or not
    #if loaded before then make left join to increment on the existing data else load the full delta
    tab_exist_flg=check_table(user_table_path)
    if tab_exist_flg=='TRUE':
        user_table_df = spark.read.json(user_delta_path).select('user_id', 'first_name', 'last_name', 'gender', 'level')
        user_join_df=user_delta_df.join(user_table_df, user_delta_df.user_id == user_table_df.user_id , 'left_outer').filter(user_table_df.user_id.isNull())
        user_insrt_df=user_join_df.select(user_delta_df.user_id, user_delta_df.first_name, user_delta_df.last_name, user_delta_df.gender, user_delta_df.level)
        user_insrt_df.write.parquet(user_table_path, 'append')
    else:
        user_insrt_df=user_delta_df.select(user_delta_df.user_id, user_delta_df.first_name, user_delta_df.last_name, user_delta_df.gender, user_delta_df.level)
        user_insrt_df.write.parquet(user_table_path, 'append')

    # create timestamp column from original timestamp column
    print('Load Time Table')
    time_table_path = '{}time.parquet'.format(output_data)
    
    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())
    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)),DateType())
    
    
    time_transf_ts_df=log_file_df.withColumn('start_time', get_timestamp('ts'))
    time_transf_dt_df = time_transf_ts_df.withColumn("datetime", get_datetime(time_transf_ts_df.ts))
    
    # extract columns to create time table
    time_delta_df =time_transf_dt_df.selectExpr( 'start_time', 'hour(datetime) as hour', 'dayofmonth(datetime) as day', 'weekofyear(datetime) as week', \
                                                'month(datetime) as month', 'year(datetime) as year', 'dayofweek(datetime) as weekday').dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    #check if the parquet file is loaded before or not
    #if loaded before then make left join to increment on the existing data else load the full delta
    tab_exist_flg=check_table(time_table_path)
    if tab_exist_flg=='TRUE':
        time_table_df = spark.read.json(time_delta_path).select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
        time_join_df=time_delta_df.join(time_table_df, time_delta_df.start_time == time_table_df.start_time , 'left_outer').filter(time_table_df.start_time.isNull())
        time_insrt_df=time_join_df.select(time_delta_df.start_time, time_delta_df.hour, time_delta_df.day,\
                                          time_delta_df.week, time_delta_df.month,time_delta_df.year,time_delta_df.weekday)
        time_insrt_df.write.partitionBy('year','month').parquet(time_table_path, 'append')
    else:
        time_insrt_df=time_delta_df.select(time_delta_df.start_time, time_delta_df.hour, time_delta_df.day,\
                                           time_delta_df.week, time_delta_df.month,time_delta_df.year,time_delta_df.weekday)
        time_insrt_df.write.partitionBy('year','month').parquet(time_table_path, 'append')

    # read in song data to use for songplays table
    print('Load Songplay table')
    songplay_table_path = '{}songplay.parquet'.format(output_data)
    song_table_path = '{}song.parquet'.format(output_data)
    artist_table_path = '{}artist.parquet'.format(output_data)

    
    # extract columns from joined song and log datasets to create songplays table 
    #get the Songs table and Artist table to be joined with the Log dataset and populate the Song_id and Artist_ID in Songsplay table
    song_tab_df = spark.read.parquet(song_table_path).select('song_id', 'title', 'artist_id', 'year', 'duration')
    artist_tab_df = spark.read.parquet(artist_table_path).select('artist_id', 'name', 'location', 'lattitude', 'longitude') 

    songplay_x_song_df =log_file_df.selectExpr('ts', 'userId as user_id', 'level', 'song' ,'artist as artist_name','sessionId as session_id',\
                                               'location','userAgent as user_agent').join(song_tab_df,log_file_df.song==song_tab_df.title,'leftouter')
    
    songplay_x_artist_df =songplay_x_song_df.select(songplay_x_song_df.ts, songplay_x_song_df.user_id , songplay_x_song_df.level, \
                                                    song_tab_df.song_id ,songplay_x_song_df.artist_name,songplay_x_song_df.session_id,songplay_x_song_df.location,\
                                                    songplay_x_song_df.user_agent).join(artist_tab_df,songplay_x_song_df.artist_name==artist_tab_df.name,'leftouter')
    songplay_x_time_df =songplay_x_artist_df.select(songplay_x_song_df.ts, songplay_x_song_df.user_id , songplay_x_song_df.level,\
                                                    song_tab_df.song_id ,artist_tab_df.artist_id,songplay_x_song_df.session_id,songplay_x_song_df.location,\
                                                    songplay_x_song_df.user_agent).withColumn('start_time', \
                                                                                              get_timestamp('ts')).withColumn('songplay_id', F.monotonically_increasing_id())
    
    songplay_delta_df=songplay_x_time_df.selectExpr('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', \
                                                    'session_id', 'location', 'user_agent','month(start_time) as month','year(start_time) as year') 
    # write songplays table to parquet files partitioned by year and month
    songplay_delta_df.write.partitionBy('year','month').parquet(songplay_table_path, 'append')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend//"
    output_data = "s3a://udacity-sparkify-model-bucket//data//"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
    spark.stop()