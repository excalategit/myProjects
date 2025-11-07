import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

# Extract to staging

def extract_transform():
    connection = None

    try:
        engine = create_engine('postgresql:///Source')

        source_table = pd.read_sql('movies', engine)

        # Remove comma-separated values
        source_table['Genre'] = source_table['Genre'].str.split(',')
        source_table = source_table.explode('Genre')
        # It good practice to strip the exploded column, this removes whitespaces that can create fake duplicates.
        # For example 'Drama' and ' Drama' won't be detected as duplicates and will cause issues later in the pipeline.
        source_table['Genre'] = source_table['Genre'].str.strip()

        # Address multiple columns of the same attribute
        source_table = pd.melt(source_table,
                     id_vars=['Poster_Link', 'Series_Title', 'Released_Year', 'Certificate',
                              'Runtime', 'Genre', 'IMDB_Rating', 'Overview', 'Meta_score', 'Director', 'No_of_Votes',
                              'Gross'],
                     value_vars=['Star1', 'Star2', 'Star3', 'Star4'],
                     var_name='Actor',
                     value_name='Actor_Name').sort_values('Series_Title', ascending=True)

        # Remove the unnecessary Actor column
        source_table.drop(columns='Actor', inplace=True)

        schema = {
            'Poster_Link': 'string',
            'Series_Title': 'string',
            'Released_Year': 'Int64',
            'Certificate': 'string',
            'Runtime': 'string',
            'Genre': 'string',
            'IMDB_Rating': 'float',
            'Overview': 'string',
            'Meta_score': 'float',
            'Director': 'string',
            'No_of_Votes': 'Int64',
            'Gross': 'Int64'
        }

        # The Gross column is currently saved as a string data type, since we intend to convert it to
        # an integer any commas must first be removed, otherwise it will throw an error.
        source_table['Gross'] = source_table['Gross'].str.replace(',', '')

        for col, dtype in schema.items():
            if dtype == 'Int64':
                source_table[col] = pd.to_numeric(source_table[col], errors="coerce").astype('Int64')
            elif dtype == 'float':
                source_table[col] = pd.to_numeric(source_table[col], errors="coerce")
            elif dtype == 'datetime':
                source_table[col] = pd.to_datetime(source_table[col], errors="coerce")

        # Group columns by dtype
        str_cols = source_table.select_dtypes(include='object').columns
        num_cols = source_table.select_dtypes(include='number').columns
        bool_cols = source_table.select_dtypes(include='bool').columns

        # Apply default fills for the data type groups
        source_table[num_cols] = source_table[num_cols].fillna(0)
        source_table[str_cols] = source_table[str_cols].fillna('NA')
        source_table[bool_cols] = source_table[bool_cols].fillna(False)

        # Remove any duplicated rows
        source_table = source_table.drop_duplicates()

        # For every record where it shows NaN, NaT, replace with None. This ensures that
        # the missing data can be loaded to a database where they will be represented as 'Nulls.'
        source_table = source_table.where(pd.notna(source_table), None)

        engine2 = create_engine('postgresql:///Destination')

        source_table.to_sql('stg_movies', engine2, index=False, if_exists='replace')

        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432) as connection:

            with connection.cursor() as cursor:
                staging_update = '''
                ALTER TABLE stg_movies
                ADD COLUMN movie_key INT,
                ADD COLUMN actor_key INT,
                ADD COLUMN director_key INT
                '''

                cursor.execute(staging_update)

        return print('Extraction to staging completed')

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()

# Create the database tables

connection = None

try:
    with psycopg2.connect(
            host='localhost',
            dbname='Destination',
            user=db_user,
            password=db_password,
            port=5432,
            options='-c search_path=movies') as connection:

        with connection.cursor() as cursor:

            # Table creation query for each table

            create_table_dim_director = '''
            CREATE TABLE IF NOT EXISTS dim_director (
            director_key SERIAL PRIMARY KEY,
            Director TEXT
            )'''

            cursor.execute(create_table_dim_director)

            create_table_dim_movie = '''
            CREATE TABLE IF NOT EXISTS dim_movie (
            movie_key SERIAL PRIMARY KEY,
            Series_Title TEXT,
            Released_Year TEXT, 
            Runtime TEXT,
            Overview TEXT,
            Meta_score TEXT,
            IMDB_Rating TEXT,
            Certificate TEXT,
            Poster_Link TEXT,
            director_key INT REFERENCES dim_director (director_key)
            )'''

            cursor.execute(create_table_dim_movie)

            create_table_dim_actor = '''
            CREATE TABLE IF NOT EXISTS dim_actor (
            actor_key SERIAL PRIMARY KEY,
            Actor_Name TEXT
            )'''

            cursor.execute(create_table_dim_actor)

            create_table_dim_genre = '''
            CREATE TABLE IF NOT EXISTS dim_genre (
            genre_key  SERIAL PRIMARY KEY,
            Genre TEXT
            )'''

            cursor.execute(create_table_dim_genre)

            create_table_director_actor = '''
            CREATE TABLE IF NOT EXISTS director_actor (
            director_key INT REFERENCES dim_director (director_key),
            actor_key INT REFERENCES dim_actor (actor_key)
            )'''

            cursor.execute(create_table_director_actor)

            create_table_actor_movie = '''
            CREATE TABLE IF NOT EXISTS actor_movie (
            actor_key INT REFERENCES dim_actor (actor_key),
            movie_key INT REFERENCES dim_movie (movie_key)
            )'''

            cursor.execute(create_table_actor_movie)

            create_table_genre_movie = '''
            CREATE TABLE IF NOT EXISTS genre_movie ( 
            genre_key INT REFERENCES dim_genre (genre_key),
            movie_key INT REFERENCES dim_movie (movie_key)
            )'''

            cursor.execute(create_table_genre_movie)

            create_table_fact_gross = '''
            CREATE TABLE IF NOT EXISTS fact_gross (
            gross_key SERIAL PRIMARY KEY,
            Gross INT,
            No_of_Votes INT,
            movie_key INT REFERENCES dim_movie (movie_key),
            actor_key INT REFERENCES dim_actor (actor_key),
            director_key INT REFERENCES dim_director (director_key)
            )'''

            cursor.execute(create_table_fact_gross)

except Exception as error:
    print(error)

finally:
    if connection is not None:
        connection.close()


# Defining the function that will perform the INSERT action when called by the ETL stages.

def insert(insert_query, dataset, table_name):
    connection = None
    loaded_rows = 0

    try:
        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432,
                options='-c search_path=movies'
                ) as connection:

            with connection.cursor() as cursor:

                psycopg2.extras.execute_batch(cursor, insert_query, dataset)
                print(f'Rows {loaded_rows} to {len(dataset)} loaded successfully for {table_name}')
                loaded_rows += len(dataset)

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')

    finally:
        if connection is not None:
            connection.close()


# Loading data to respective tables

def load_dim_director():
    table_name = 'dim_director'
    try:

        engine = create_engine('postgresql:///Destination')

        dd = pd.read_sql('stg_movies', engine)

        directors = dd[['Director']]
        directors = directors.drop_duplicates()
        directors = directors.to_dict('records')

        insert_query = '''
        INSERT INTO dim_director (
        Director
        )
        VALUES (
        %(Director)s
        )'''

        insert(insert_query, directors, table_name)

    except Exception as error:
        print(error)


def load_dim_actor():
    table_name = 'dim_actor'
    try:

        engine = create_engine('postgresql:///Destination')

        da = pd.read_sql('stg_movies', engine)

        actors = da[['Actor_Name']]
        actors = actors.drop_duplicates()
        actors = actors.to_dict('records')

        insert_query = '''
        INSERT INTO dim_actor (
        Actor_Name
        )
        VALUES (
        %(Actor_Name)s
        )'''

        insert(insert_query, actors, table_name)

    except Exception as error:
        print(error)


def load_dim_movies():
    table_name = 'dim_movie'
    try:

        engine = create_engine('postgresql:///Destination')

        dm = pd.read_sql('stg_movies', engine)
        movies = dm[['Series_Title', 'Released_Year', 'Runtime', 'Overview',
                     'Meta_score', 'IMDB_Rating', 'Certificate', 'Poster_Link']]
        movies = movies.drop_duplicates(subset=['Series_Title', 'Released_Year'], keep='first')
        movies = movies.to_dict('records')

        insert_query = '''
        INSERT INTO dim_movie ( 
        Series_Title, 
        Released_Year, 
        Runtime,
        Overview,
        Meta_score,
        IMDB_Rating,
        Certificate,
        Poster_Link
        )
        VALUES (
        %(Series_Title)s, 
        %(Released_Year)s,
        %(Runtime)s,
        %(Overview)s, 
        %(Meta_score)s,
        %(IMDB_Rating)s,
        %(Certificate)s,
        %(Poster_Link)s 
        )
        '''

        insert(insert_query, movies, table_name)

    except Exception as error:
        print(error)


def load_dim_genre():
    table_name = 'dim_genre'
    try:

        engine = create_engine('postgresql:///Destination')

        dg = pd.read_sql('stg_movies', engine)

        # genre = dg[['Genre']].copy()
        # genre['Genre'] = genre['Genre'].str.strip()
        genre = dg[['Genre']]
        genre = genre.drop_duplicates()
        genre = genre.to_dict('records')

        insert_query = '''
        INSERT INTO dim_genre (
        Genre
        )
        VALUES (
        %(Genre)s
        )'''

        insert(insert_query, genre, table_name)

    except Exception as error:
        print(error)

# Loading surrogate keys from dimension tables to staging.

def load_surrogate_keys():
    connection = None

    try:

        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432) as connection:

            with connection.cursor() as cursor:

                movie_key = '''
                UPDATE stg_movies AS s
                SET movie_key = m.movie_key
                FROM movies.dim_movie AS m
                WHERE s."Series_Title" = m.series_title
                AND s."Released_Year" = m.released_year
                '''
                cursor.execute(movie_key)

                actor_key = '''
                UPDATE stg_movies AS s
                SET actor_key = a.actor_key
                FROM movies.dim_actor AS a
                WHERE s."Actor_Name" = a.actor_name
                '''
                cursor.execute(actor_key)

                director_key = '''
                UPDATE stg_movies AS s
                SET director_key = d.director_key
                FROM movies.dim_director AS d
                WHERE s."Director" = d.director
                '''
                cursor.execute(director_key)

                update_dim_movie = '''
                UPDATE movies.dim_movie AS m
                SET director_key = s.director_key
                FROM stg_movies AS s
                WHERE m.movie_key = s.movie_key
                '''
                cursor.execute(update_dim_movie)

                # Loading surrogate keys from staging table to join tables.
                load_director_actor = '''
                INSERT INTO movies.director_actor (director_key, actor_key)
                SELECT DISTINCT director_key, actor_key
                FROM stg_movies
                '''
                cursor.execute(load_director_actor)

                load_actor_movie = '''
                INSERT INTO movies.actor_movie (actor_key, movie_key)
                SELECT DISTINCT actor_key, movie_key
                FROM stg_movies
                '''
                cursor.execute(load_actor_movie)

                load_genre_movie = '''
                INSERT INTO movies.genre_movie (genre_key, movie_key)
                SELECT DISTINCT genre_key, movie_key
                FROM stg_movies as s
                join movies.dim_genre as g on s."Genre" = g.genre
                '''
                cursor.execute(load_genre_movie)

                return print('All target tables updated with surrogate keys successfully.')

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


def load_fact_gross():
    table_name = 'fact_gross'
    try:

        engine = create_engine('postgresql:///Destination')

        df = pd.read_sql('stg_movies', engine)

        fact = df[['Gross', 'No_of_Votes', 'movie_key', 'actor_key', 'director_key']]
        fact = fact.drop_duplicates()
        fact = fact.to_dict('records')

        insert_query = '''
        INSERT INTO fact_gross (
        Gross,
        No_of_Votes,
        movie_key,
        actor_key,
        director_key
        )
        VALUES (
        %(Gross)s,
        %(No_of_Votes)s,
        %(movie_key)s,
        %(actor_key)s,
        %(director_key)s
        )
        '''

        insert(insert_query, fact, table_name)

    except Exception as error:
        print(error)


extract_transform()

load_dim_director()

load_dim_actor()

load_dim_movies()

load_dim_genre()

load_surrogate_keys()

load_fact_gross()