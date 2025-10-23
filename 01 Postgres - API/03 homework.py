# Here, some transformations are required to the data before loading to staging because:
# - some of the data are in the form of dictionaries which cannot be loaded in its present form to a Postgres database.
# - some of the data are needed in a transformed form for 1:1 comparison with dimension tables later.


import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
from dotenv import load_dotenv

prd_url = 'https://fakestoreapi.com/products'
sale_url = 'https://fakestoreapi.com/carts'
user_url = 'https://fakestoreapi.com/users'


def prd_scraper(prd_url):
    try:
        response = requests.get(prd_url).json()
        prd_df = pd.DataFrame(response)
        prd_df_expand = prd_df.join(pd.json_normalize(prd_df['rating']))
        prd_df_expand = prd_df_expand.drop(columns=['rating'])
        
        engine = create_engine('postgresql:///Destination')
        prd_df_expand.to_sql('stg_product_table', engine, index=False, if_exists='fail')
        return print('Product data successfully extracted and loaded to staging')
        
    except Exception as error:
        print(error)


def sale_scraper(sale_url):
    try:
        response = requests.get(sale_url).json()
        sale_df = pd.DataFrame(response)
        sale_explode = sale_df.explode('products')
        products_expanded = sale_explode['products'].apply(pd.Series)
        sale_df_expanded = pd.concat([sale_explode, products_expanded], axis=1)
        # It appears the join command is not working as expected to combine the original df
        # and the normalized data, probably due to an earlier explosion step (because that's the
        # only additional step making it different from the usage of 'join' in the function above,
        # where join was used successfully). Therefore, concat is used here instead.
        sale_df_expanded = sale_df_expanded.drop(columns=['products'])
        sale_df_expanded['date'] = pd.to_datetime(sale_df_expanded['date']).dt.date
        sale_df_expanded['month'] = pd.to_datetime(sale_df_expanded['date']).dt.month
        sale_df_expanded['year'] = pd.to_datetime(sale_df_expanded['date']).dt.year
        
        engine = create_engine('postgresql:///Destination')
        sale_df_expanded.to_sql('stg_sale_table', engine, index=False, if_exists='fail')
        return print('Sales data successfully extracted and loaded to staging')
        
    except Exception as error:
        print(error)


def user_scraper(user_url):
    try:
        response = requests.get(user_url).json()
        user_df = pd.DataFrame(response)
        user_df_expand1 = user_df.join(pd.json_normalize(user_df['address']))
        user_df_expand2 = user_df_expand1.join(pd.json_normalize(user_df_expand1['name']))
        user_df_expand2 = user_df_expand2.drop(columns=['address'])
        user_df_expand2 = user_df_expand2.drop(columns=['name'])
        # The transformation below is required at this step instead of later only because 
        # the data is required to already be in its cleaned form in staging for the 
        # comparison done in the load_surrogate-keys() function to work.
        user_df_expand2['firstname'] = user_df_expand2['firstname'].str.capitalize()
        user_df_expand2['lastname'] = user_df_expand2['lastname'].str.capitalize()
        
        engine = create_engine('postgresql:///Destination')
        user_df_expand2.to_sql('stg_user_table', engine, index=False, if_exists='fail')
        return print('User data successfully extracted and loaded to staging')
    
    except Exception as error:
        print(error)

# Creating a combined staging table from all 3 data sources

load_dotenv()
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')

connection = None

try:
    
    with psycopg2.connect(
            host='localhost',
            dbname='Destination',
            user=db_user,
            password=db_password,
            port=5432
            ) as connection:
    
        with connection.cursor() as cursor:
            
            create_combined_stg_table = '''
            CREATE TABLE stg_combo_table AS
            SELECT a.id, "userId", date, month, year, a.__v, "productId", quantity, title, price, 
            description, category, image, rate, count, email, username, 
            password, phone, city, street, number, zipcode, "geolocation.lat", 
            "geolocation.long", firstname, lastname 
            FROM stg_sale_table AS a
            JOIN stg_product_table AS b on a."productId" = b.id
            JOIN stg_user_table AS c on a."userId" = c.id
            '''
            
            cursor.execute(create_combined_stg_table) 

            # Creation of additional columns on staging to hold surrogate keys
            # from dim tables.
            update_staging = '''
            ALTER TABLE stg_combo_table
            ADD COLUMN product_key INT,
            ADD COLUMN user_key INT,
            ADD COLUMN date_key INT
            '''
            
            cursor.execute(update_staging) 
            
except Exception as error:
    print(error)
    
finally:
    if connection is not None:
        connection.close()

# Creating the target tables

connection = None
    
try:
    
    with psycopg2.connect(
            host='localhost',
            dbname='Destination',
            user=db_user,
            password=db_password,
            port=5432,
            options='-c search_path=homework'
            ) as connection:
    
        with connection.cursor() as cursor:

            create_dim_product = '''
            CREATE TABLE IF NOT EXISTS dim_product (
            product_key SERIAL PRIMARY KEY,
            product_id INT,
            product_name TEXT,
            description TEXT,
            category TEXT,
            image TEXT,
            rating INT
            )'''
            
            cursor.execute(create_dim_product)

            create_dim_city = '''
            CREATE TABLE IF NOT EXISTS dim_city (
            city_key SERIAL PRIMARY KEY,
            city TEXT
            )'''

            cursor.execute(create_dim_city)

            create_dim_user = '''
            CREATE TABLE IF NOT EXISTS dim_user (
            user_key SERIAL PRIMARY KEY,
            user_id INT UNIQUE,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            username TEXT,
            password TEXT,
            phone TEXT,
            street TEXT,
            number INT,
            zipcode TEXT,
            latitude TEXT,
            longitude TEXT,
            city_key INT REFERENCES dim_city (city_key)
            )'''
            
            cursor.execute(create_dim_user)
            
            create_dim_date = '''
            CREATE TABLE IF NOT EXISTS dim_date (
            date_key SERIAL PRIMARY KEY,
            sale_date DATE,
            month INT,
            year INT
            )'''
            
            cursor.execute(create_dim_date) 
            
            create_fact_table = '''
            CREATE TABLE IF NOT EXISTS sale_fact_table (
            sale_key SERIAL PRIMARY KEY,
            sale_id INT,
            product_key INT REFERENCES dim_product (product_key),
            user_key INT REFERENCES dim_user (user_key),
            date_key INT REFERENCES dim_date (date_key),
            price FLOAT,
            quantity INT,
            total_sale FLOAT,
            stock INT
            )'''
            
            cursor.execute(create_fact_table)
            
except Exception as error:
    print(error)
    
finally:
    if connection is not None:
        connection.close()

# Defining the function that will perform the INSERT action when called by the ETL stages.


def insert(insert_query, dataset):
    connection = None
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    try:
        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432,
                options='-c search_path=homework'
                ) as connection:

            with connection.cursor() as cursor:

                psycopg2.extras.execute_batch(cursor, insert_query, dataset)

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()

# Defining the functions that extracts each table from staging, transforms, and loads to target


def transform_load_dim_product():
    try:

        engine = create_engine('postgresql:///Destination')

        dp = pd.read_sql('stg_combo_table', engine)
        product = dp[['productId', 'title', 'description', 'category', 'image', 'rate']].copy()
        product = product.rename(columns={'productId':'product_id', 'title':'product_name', 'rate':'rating'})
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')
        product = product.to_dict('records')

        insert_query = '''INSERT into dim_product (
        product_id,
        product_name,
        description,
        category,
        image,
        rating
        ) 
        VALUES (
        %(product_id)s,
        %(product_name)s,
        %(description)s,
        %(category)s,
        %(image)s,
        %(rating)s
        )'''

        insert(insert_query, product)
        return print('dim_product loaded successfully')
    
    except Exception as error:
        print(error)


def transform_load_dim_user():
    try:

        engine = create_engine('postgresql:///Destination')

        du = pd.read_sql('stg_combo_table', engine)
        user = du[['userId', 'email', 'username', 'password', 'phone', 'firstname', 'lastname',
                   'street', 'number', 'zipcode', 'geolocation.lat', 'geolocation.long']].copy()
        user = user.rename(columns={'userId':'user_id', 'geolocation.lat': 'latitude', 'geolocation.long': 'longitude'})
        user['email'] = '***Masked***'
        user['username'] = '***Masked***'
        user['password'] = '***Masked***'
        user['phone'] = '***Masked***'
        user['street'] = user['street'].str.title()
        user = user.drop_duplicates(subset=['user_id', 'firstname', 'lastname'], keep='first')
        user = user.to_dict('records')

        insert_query = '''INSERT into dim_user (
        user_id,
        first_name,
        last_name,
        email,
        username,
        password,
        phone,
        street,
        number,
        zipcode,
        latitude,
        longitude
        ) 
        VALUES (
        %(user_id)s,
        %(firstname)s,
        %(lastname)s,
        %(email)s,
        %(username)s,
        %(password)s,
        %(phone)s,
        %(street)s,
        %(number)s,
        %(zipcode)s,
        %(latitude)s,
        %(longitude)s
        )'''

        insert(insert_query, user)
        return print('dim_user loaded successfully')
    
    except Exception as error:
        print(error)


def transform_load_dim_date():
    try:

        engine = create_engine('postgresql:///Destination')

        dt = pd.read_sql('stg_combo_table', engine)
        date = dt[['date', 'month', 'year']].copy()
        date = date.rename(columns={'date':'sale_date'})
        date = date.drop_duplicates(subset=['sale_date'], keep='first')
        date = date.to_dict('records')

        insert_query = '''INSERT into dim_date (
        sale_date,
        month,
        year
        ) 
        VALUES (
        %(sale_date)s,
        %(month)s,
        %(year)s
        )'''

        insert(insert_query, date)
        return print('dim_date loaded successfully')
    
    except Exception as error:
        print(error)
        
# Defining the function that loads the surrogate keys of each dimension table to staging


def load_surrogate_keys():
    connection = None
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
        
    try:
        
        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432) as connection:

            with connection.cursor() as cursor:
                # Loading surrogate keys from dimension tables to staging.

                product_key = '''UPDATE stg_combo_table AS c SET product_key = p.product_key 
                FROM homework.dim_product AS p WHERE c."productId" = p.product_id AND
                c.title = p.product_name'''
                cursor.execute(product_key)
                
                user_key = '''UPDATE stg_combo_table AS c SET user_key = u.user_key 
                FROM homework.dim_user AS u WHERE c."userId" = u.user_id AND
                c.firstname = u.first_name AND c.lastname = u.last_name'''
                cursor.execute(user_key)
                
                date_key = '''UPDATE stg_combo_table AS c SET date_key = d.date_key 
                FROM homework.dim_date AS d WHERE c.date = d.sale_date'''
                cursor.execute(date_key)

                # Fetching surrogate keys from dim_city and loading to dim_user.
                # Note that since there is only one attribute and no transformation required
                # it is more efficient loading dim_city this way than through a dataframe.
                # This also explains why its loading is designed to happen at this point i.e. together with
                # similar sql commands.

                load_dim_city = '''INSERT INTO homework.dim_city (city)
                SELECT DISTINCT city FROM stg_combo_table'''
                cursor.execute(load_dim_city)

                update_dim_user = '''UPDATE homework.dim_user AS u SET city_key = c.city_key 
                FROM stg_combo_table AS s
                JOIN homework.dim_city AS c ON s.city = c.city
                WHERE u.user_id = s."userId" '''
                cursor.execute(update_dim_user)
                
                return print('All target tables updated with surrogate keys successfully.')
    
    except Exception as error:
        print(error)
        
    finally:
        if connection is not None:
            connection.close()

# Finally, defining the function that transforms and loads the fact data together with all 
# surrogate keys to the fact table.


def transform_load_fact_table():
    try:

        engine = create_engine('postgresql:///Destination')

        df = pd.read_sql('stg_combo_table', engine)
        fact = df[['id', 'product_key', 'user_key', 'date_key', 'price', 'quantity', 'count']].copy()
        fact = fact.rename(columns={'id':'sale_id', 'count':'stock'})
        fact['total_sale'] = fact['price'] * fact['quantity']
        fact = fact.to_dict('records')

        insert_query = '''INSERT into sale_fact_table (
        sale_id,
        product_key,
        user_key,
        date_key,
        price,
        quantity,
        total_sale,
        stock
        ) 
        VALUES (
        %(sale_id)s,
        %(product_key)s,
        %(user_key)s,
        %(date_key)s,
        %(price)s,
        %(quantity)s,
        %(total_sale)s,
        %(stock)s
        )'''

        insert(insert_query, fact)
        return print('fact_table loaded successfully')
    
    except Exception as error:
        print(error)


prd_scraper(prd_url)

sale_scraper(sale_url)

user_scraper(user_url)

transform_load_dim_product()

transform_load_dim_user()

transform_load_dim_date()

load_surrogate_keys()

transform_load_fact_table()