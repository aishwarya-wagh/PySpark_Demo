from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.conf import *
from pyspark.sql.column import *


spark = SparkSession.builder.appName("basic").getOrCreate()

employee = spark.read.format("csv").load("../data/employees.csv",inferSchema=True, header=True)
departments = spark.read.format("csv").load("../data/departments.csv", inferSchema=True, header=True)


joined_df = employee.join(departments, how="inner", on='DepartmentID')

employee.show(5)
departments.show(5)
joined_df.show(5)