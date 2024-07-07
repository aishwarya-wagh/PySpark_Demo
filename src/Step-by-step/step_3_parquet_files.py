# https://campus.datacamp.com/courses/cleaning-data-with-pyspark/dataframe-details?ex=8

# Saving a DataFrame in Parquet format
# When working with Spark, you'll often start with CSV, JSON, or other data sources. This provides a lot of flexibility for the types of data to load, but it is not an optimal format for Spark. The Parquet format is a columnar data store, allowing Spark to use predicate pushdown. This means Spark will only process the data necessary to complete the operations you define versus reading the entire dataset. This gives Spark more flexibility in accessing the data and often drastically improves performance on large datasets.
#
# In this exercise, we're going to practice creating a new Parquet file and then process some data from it.
#
# The spark object and the df1 and df2 DataFrames have been setup for you.
#
# Instructions
# 100 XP
# View the row count of df1 and df2.
# Combine df1 and df2 in a new DataFrame named df3 with the union method.
# Save df3 to a parquet file named AA_DFW_ALL.parquet.
# Read the AA_DFW_ALL.parquet file and show the count.

from pyspark.sql import *
from pyspark.sql.functions import  *
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

schema1= StructType([StructField('name',StringType(),True)])
data1=[("aish"),("wagh")]

df1 = spark.createDataFrame(data1,schema1)

schema2 = StringType([StructField('age',IntegerType(), True)])
data2= ['22','55']

df2=spark.createDataFrame(data2,schema2)
# View the row count of df1 and df2
print("df1 Count: %d" % df1.count())
print("df2 Count: %d" % df2.count())

# Combine the DataFrames into one
df3 = df1.union(df2)

# Save the df3 DataFrame in Parquet format
df3.write.parquet('AA_DFW_ALL.parquet', mode='overwrite')

# Read the Parquet file into a new DataFrame and run a count
print(spark.read.parquet('AA_DFW_ALL.parquet').count())