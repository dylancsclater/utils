from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("PySpark SQL").getOrCreate()

# Create a DataFrame from a CSV file
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)

# Show the first 5 rows of the DataFrame
df.show(5)

# Print the DataFrame's schema
df.printSchema()

# Select a column
df.select("column_name").show()

# Filter rows based on a condition
df.filter(df["column_name"] > value).show()

# Group the DataFrame by a column and compute the mean of another column
df.groupBy("column_name1").mean("column_name2").show()

# Sort the DataFrame by a column
df.sort("column_name", ascending=False).show()

# Perform a join between two DataFrames
df1 = spark.read.csv("path/to/file1.csv", header=True, inferSchema=True)
df2 = spark.read.csv("path/to/file2.csv", header=True, inferSchema=True)
df3 = df1.join(df2, df1["column_name"] == df2["column_name"])
df3.show()

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("table_name")

# Perform a SQL query on the table
result = spark.sql("SELECT column_name FROM table_name WHERE column_name > value")
result.show()

