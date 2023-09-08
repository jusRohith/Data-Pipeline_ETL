import requests
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine


# Function to extract data from an API
def extract_data() -> dict:
    '''This function extracts data from http://universities.hipolabs.com/search?country=United+States'''
    url = 'http://universities.hipolabs.com/search?country=United+States'
    api_data = None
    try:
        api_data = requests.get(url)
        api_data.raise_for_status()
    except Exception as e:
        print(e)
    return api_data.json()


# Function to transform the raw data into a DataFrame
def transform_data(raw_data: dict) -> pd.DataFrame:
    data_df = pd.DataFrame(raw_data)

    # Filter universities located in California
    data_df = data_df[data_df['name'].str.contains('California')]

    # Convert lists to comma-separated strings for 'domains' and 'web_pages' columns
    data_df['domains'] = [','.join(map(str, l)) for l in data_df['domains']]
    data_df['web_pages'] = [','.join(map(str, l)) for l in data_df['web_pages']]

    # Reset the DataFrame index
    data_df = data_df.reset_index(drop=True)

    # Select specific columns and return the resulting DataFrame
    return data_df[['domains', 'country', 'web_pages', 'name']]


# Function to load the transformed data into a PostgreSQL database
def load(table_data: pd.DataFrame) -> None:
    engine = create_engine("postgresql+psycopg2://postgres:Postgres@localhost/PythonCreated")

    # Save the DataFrame as a table in the PostgreSQL database
    table_data.to_sql('usa_universities_data', engine, if_exists='replace', index=False)


# Extract data from the API
data = extract_data()

# Transform the raw data into a DataFrame
df = transform_data(data)

# Load the transformed data into a PostgreSQL database
load(df)
