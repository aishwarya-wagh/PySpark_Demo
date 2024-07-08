# https://zillacode.com/ide/1

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

def etl(input_df):
    input_df=input_df.filter(input_df.view_count>1000000).filter(input_df.release_year>2018)
    return input_df

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()
input_df = spark.read.format("csv").load("../data/video_data.csv",header=True, inferSchema=True)
input_df.show()
output_df =etl(input_df)
output_df.show()


