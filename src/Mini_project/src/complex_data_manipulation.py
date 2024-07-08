from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("ComplexDataManipulation").getOrCreate()

# Sample DataFrames (In practice, these would be loaded from data sources)
departments_data = [(1, "HR", 101), (2, "Finance", 102), (3, "IT", 103)]
deparment_schema = StructType([StructField("DId", IntegerType(),False),
                               StructField("departmentName", StringType(),True),
                                StructField("managerId", IntegerType(),True)
                               ])


employees_data = [(101, "Alice", 30, 1, "USA", 70000, "2021-01-01", None),
                  (102, "Bob", 35, 2, "UK", 80000, "2020-01-01", None),
                  (103, "Charlie", 40, 3, "USA", 90000, "2019-01-01", None),
                  (104, "David", 28, 3, "Canada", 75000, "2022-01-01", None)]
projects_data = [(1001, "ProjectX", 101, "2021-06-01", "2021-12-01", "Completed"),
                 (1002, "ProjectY", 102, "2020-03-01", "2020-09-01", "Completed"),
                 (1003, "ProjectZ", 103, "2019-04-01", "2020-04-01", "Completed"),
                 (1004, "ProjectA", 104, "2022-02-01", "2022-08-01", "Ongoing")]

departments_df = spark.createDataFrame(departments_data, deparment_schema)
employees_df = spark.createDataFrame(employees_data, ["EmployeeID", "Name", "Age", "DepartmentID", "Country", "Salary", "HireDate", "NullableColumn"])
projects_df = spark.createDataFrame(projects_data, ["ProjectID", "ProjectName", "EmployeeID", "StartDate", "EndDate", "Status"])

# Join employees and departments
join_criteria= "employees_df.DeparmentId=departments_df.DId"
#emp_dept_df = employees_df.join(departments_df, on=join_criteria, how="inner")

# Join the result with projects
emp_dept_proj_df = employees_df.join(projects_df, "EmployeeID")

# Filter: Only include employees from the USA
usa_employees_df = emp_dept_proj_df.filter(emp_dept_proj_df.Country == "USA")

# Aggregation: Calculate average salary per department
avg_salary_per_dept = usa_employees_df.groupBy("DepartmentName").agg(avg("Salary").alias("AverageSalary"))

# Window function: Rank employees by salary within each department
window_spec = Window.partitionBy("DepartmentID").orderBy(col("Salary").desc())
ranked_employees_df = usa_employees_df.withColumn("Rank", row_number().over(window_spec))

# Filtering: Only include top 2 highest-paid employees per department
top_2_employees_per_dept = ranked_employees_df.filter(col("Rank") <= 2)

# Select and show relevant columns
result_df = top_2_employees_per_dept.select("Name", "DepartmentName", "Salary", "Rank", "ProjectName", "Status")

result_df.show()
