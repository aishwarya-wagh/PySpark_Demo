from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, to_date, year, month, dayofmonth, udf, col
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark Demo") \
    .getOrCreate()

# Load CSV data into DataFrame
df = spark.read.csv("../data/sample_data.csv", header=True, inferSchema=True)

# Filtering Data
filtered_df = df.filter(df.age > 30)

# Selecting Columns
selected_df = df.select("name", "age")

# Renaming Columns
renamed_df = df.withColumnRenamed("age", "years")

# Using Alias for Columns
# Rename 'age' to 'years' using alias
aliased_df = df.select(col("age").alias("years"))

# Creating new columns with alias
# Add 10 to the 'age' column and name it 'age_plus_10' using alias
aliased_new_col_df = df.select(col("name"), col("age"), (col("age") + 10).alias("age_plus_10"))
# Show DataFrames
aliased_df.show()
aliased_new_col_df.show()
# Adding Columns
df_with_new_col = df.withColumn("age_plus_10", df.age + 10)

# Dropping Columns
df_dropped_col = df.drop("age")

# Aggregating Data
avg_age = df.agg(avg("age")).collect()[0][0]

# Joining DataFrames
# Sample DataFrame to join
df2 = df.select("name", "salary")
joined_df = df.join(df2, on="name", how="inner")

# Handling Missing Values
df_dropped_na = df.dropna()
df_filled_na = df.fillna({"age": avg_age})

# Sorting Data
sorted_df = df.orderBy(df.age.desc())

# Window Functions
window_spec = Window.partitionBy("department").orderBy(df.salary.desc())
df_with_rank = df.withColumn("rank", rank().over(window_spec))

# GroupBy and Aggregations
grouped_df = df.groupBy("department").agg(avg("salary").alias("avg_salary"))

# Pivoting and Unpivoting
pivoted_df = df.groupBy("department").pivot("gender").agg(avg("salary"))

# Handling Dates and Timestamps
df_with_date = df.withColumn("date", to_date(df.date_string, "yyyy-MM-dd"))
df_with_date_parts = df_with_date.withColumn("year", year(df_with_date.date)) \
    .withColumn("month", month(df_with_date.date)) \
    .withColumn("day", dayofmonth(df_with_date.date))

# User Defined Functions (UDFs)
def to_uppercase(name):
    return name.upper()

uppercase_udf = udf(to_uppercase, StringType())
df_with_uppercase_name = df.withColumn("name_uppercase", uppercase_udf(df.name))


df_with_uppercase_name.show()

# Stop Spark session
spark.stop()
