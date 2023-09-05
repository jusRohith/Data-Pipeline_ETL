import sqlalchemy, psycopg2
import requests
import pandas as pd

# Define the URL and headers for the API request
URL = 'http://api.coincap.io/v2/assets'
HEADERS = {'Content-Type': 'application/json', 'Accept-Encoding': 'deflate'}

# Initialize the response variable
response = None

try:
    # Send an HTTP GET request to the specified URL with the headers
    response = requests.get(URL, headers=HEADERS)
    response.raise_for_status()  # Raise an exception if the request was not successful
except Exception as e:
    print(e)  # Print any exception that occurred during the request

# Parse the JSON response
response_json = response.json()

# Normalize the JSON data into a Pandas DataFrame
df = pd.json_normalize(response_json, 'data')

# Print the DataFrame (optional, for debugging purposes)
print(df)

# Create a SQLAlchemy database engine to connect to PostgreSQL
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:Postgres@localhost/PythonCreated')

# Write the DataFrame to a PostgreSQL table named 'fact_crypto'
# If the table already exists, it will fail (if_exists='fail')
df.to_sql(name='fact_crypto', con=engine, index=False, if_exists='fail')
