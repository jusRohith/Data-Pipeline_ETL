import pandas as pd
import sqlalchemy as sa

df = pd.read_excel(
    '/Users/rohith/RohithsWorkRelated/DataEngineerRelated/PythonRelated/'
    'Python_Related/Python Practice/output/IMDB_Movie_Ratings.xlsx', engine='openpyxl')

engine = sa.create_engine('postgresql+psycopg2://postgres:Postgres@localhost/PythonCreated')
df.to_sql('imdb_top_rated_movies', con=engine, if_exists='fail', index=False)
