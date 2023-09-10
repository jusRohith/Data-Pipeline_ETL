# Import required libraries
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

# Define the URL for data extraction
URL = 'http://universities.hipolabs.com/search?country=United+States'
data = None

# Initialize a Spark session for data processing
spark = (SparkSession
         .builder
         .master('local[*]')  # Use all available CPU cores locally
         .appName('USA_colleges_data')  # Set the application name
         .getOrCreate())

# Extraction
try:
    # Make an HTTP request to the URL to fetch data
    data = requests.get(URL)
    data.raise_for_status()  # Check for any HTTP request errors
except Exception as e:
    print(e)

# Parse the JSON response from the API
response_json = data.json()

# Transformation
# Create a DataFrame from the JSON response
df = spark.createDataFrame(response_json)

# Rename the 'alpha_two_code' column to 'country_code'
df = df.withColumnRenamed('alpha_two_code', 'country_code')

# we should not use this with column in for loops,calling multiple times
# may get perfomance issues and StackOverFlowException
# instead use select with multiple columns for better performance

# rename column names Approach1
# Concatenate multiple 'web_pages' values into a single string, separated by commas
df = df.withColumn('web_pages', concat_ws(',', 'web_pages'))

# rename column names Approach2
# Select specific columns and concatenate multiple 'domains' values into a single string, aliased as 'domains'
df = df.select('country_code', 'country', 'name', 'state-province', 'web_pages',
               concat_ws(',', df['domains']).alias('domains'))

# Fill any missing values with 'N/A'
df = df.fillna('N/A')

# Load
# Write the DataFrame to a PostgreSQL database table

# try:
df.write.jdbc("jdbc:postgresql://localhost:5432/PythonCreated", "california_universities",
              properties={"user": "postgres", "password": "Postgres"})
# except SQLException as e:
# print(e)

# Print a completion message
print('Loading Completed')

# Stop the Spark session to release resources
spark.stop()
