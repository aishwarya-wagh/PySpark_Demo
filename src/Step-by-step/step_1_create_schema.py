# Defining a schema
# Creating a defined schema helps with data quality and import performance. As mentioned
# during the lesson, we'll create a simple schema to read in the following columns:
#
# Name
# Age
# City
# The Name and City columns are StringType() and the Age column is an IntegerType().
#
# Instructions
# 100 XP
# Import * from the pyspark.sql.types library.
# Define a new schema using the StructType method.
# Define a StructField for name, age, and city. Each field should correspond to the correct
# datatype and not be nullable.

from pyspark.sql.types import *

people_schema =StructType ([
  StructField('name', StringType(), False),
StructField('age', IntegerType(), False),
StructField('city', StringType(),False)
])

print(people_schema)