import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Creating the fact and dimension tables where the transformed data will be loaded to.
# Here the connection is defined with an additional specification for the schema since here
# the tables are not created on the default public schema.

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
            options='-c search_path=amazon') as connection:
        # This is how to indicate the schema of interest

        with connection.cursor() as cursor:
            create_dim_product = '''
            CREATE TABLE IF NOT EXISTS dim_product (
            product_key SERIAL PRIMARY KEY,
            product_id TEXT,
            product_name TEXT,
            category TEXT,
            about_product TEXT,
            img_link TEXT,
            product_link TEXT,
            rating TEXT,
            rating_count TEXT
            )'''

            cursor.execute(create_dim_product)

            create_dim_user = '''
            CREATE TABLE IF NOT EXISTS dim_user (
            user_key SERIAL PRIMARY KEY,
            user_id TEXT,
            user_name TEXT
            )'''

            cursor.execute(create_dim_user)

            create_dim_review = '''
            CREATE TABLE IF NOT EXISTS dim_review (
            review_key SERIAL PRIMARY KEY,
            review_id TEXT,
            review_content TEXT,
            product_key INT REFERENCES dim_product (product_key)
            )'''

            cursor.execute(create_dim_review)

            create_join_table = '''
            CREATE TABLE IF NOT EXISTS product_user_join (
            product_key INT REFERENCES dim_product (product_key),
            user_key INT REFERENCES dim_user (user_key),
            PRIMARY KEY (product_key, user_key)
            )'''

            cursor.execute(create_join_table)

            create_fact_price = '''
            CREATE TABLE IF NOT EXISTS fact_price (
            price_key SERIAL PRIMARY KEY,
            "actual_price (PLN)" FLOAT,
            "discounted_price (PLN)" FLOAT,
            discount_percentage TEXT,
            product_key INT REFERENCES dim_product (product_key)
            )'''

            cursor.execute(create_fact_price)

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
                port=5432) as connection:

            with connection.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, insert_query, dataset)

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


# Defining the function that extracts and transforms to staging.

def extract_transform():
    connection = None

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_excel('amazon.xlsx')
        source_table = source_table.head(5)
        source_table = source_table.copy()
        source_table['user_id'] = source_table['user_id'].str.split(',')
        source_table['user_name'] = source_table['user_name'].str.split(',')
        source_table['review_id'] = source_table['review_id'].str.split(',')
        source_table['review_title'] = source_table['review_title'].str.split(',')
        source_table = source_table.explode(['user_id', 'user_name', 'review_id', 'review_title'])
        source_table.to_sql('stg_amazon_sales_report', engine, index=False, if_exists='replace')

        # Addition of surrogate key columns to staging.
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASS')

        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432) as connection:

            with connection.cursor() as cursor:
                staging_update = '''ALTER TABLE stg_amazon_sales_report
                ADD COLUMN product_key INT,
                ADD COLUMN user_key INT,
                ADD COLUMN review_key INT
                '''

                cursor.execute(staging_update)

                return print('Extraction to staging completed')

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


# Defining the function that loads the transformed data to the dimension tables
# starting with the sub-dimensions.

def load_dim_review():
    engine = create_engine('postgresql:///Destination')

    drr = pd.read_sql('stg_amazon_sales_report', engine)
    review = drr[['review_id', 'review_title']].copy()
    review = review.rename(columns={'review_title': 'review_content'})
    review = review.drop_duplicates(subset=['review_id'], keep='first')
    review = review.to_dict('records')

    insert_query = '''INSERT into amazon.dim_review (
    review_id,
    review_content
    )
    VALUES (
    %(review_id)s,
    %(review_content)s
    )'''

    insert(insert_query, review)
    return print('dim_review loaded successfully')


def load_dim_user():
    engine = create_engine('postgresql:///Destination')

    du = pd.read_sql('stg_amazon_sales_report', engine)
    user = du[['user_id', 'user_name']].copy()
    user = user.drop_duplicates()
    user = user.to_dict('records')

    insert_query = '''INSERT into amazon.dim_user (
    user_id,
    user_name
    ) 
    VALUES (
    %(user_id)s,
    %(user_name)s
    )'''

    insert(insert_query, user)
    return print('dim_user loaded successfully')


def load_dim_product():
    engine = create_engine('postgresql:///Destination')

    dp = pd.read_sql('stg_amazon_sales_report', engine)
    product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                  'rating', 'rating_count']].copy()
    product['rating_count'] = product['rating_count'].fillna(1)
    product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')
    # It is best practice to deduplicate dim tables using business keys alone.
    product = product.to_dict('records')

    insert_query = '''INSERT into amazon.dim_product (
    product_id,
    product_name,
    category,
    about_product,
    img_link,
    product_link,
    rating,
    rating_count
    ) 
    VALUES (
    %(product_id)s,
    %(product_name)s,
    %(category)s,
    %(about_product)s,
    %(img_link)s,
    %(product_link)s,
    %(rating)s,
    %(rating_count)s
    )'''

    insert(insert_query, product)
    return print('dim_product loaded successfully')


# Defining the function that fetches and loads surrogate keys to their respective target tables.

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
                # Loading surrogate keys from dim tables to staging.

                product_key = '''UPDATE stg_amazon_sales_report AS s SET product_key = p.product_key 
                FROM amazon.dim_product AS p WHERE s.product_id = p.product_id AND
                s.product_name = p.product_name'''
                cursor.execute(product_key)

                user_key = '''UPDATE stg_amazon_sales_report AS s SET user_key = u.user_key 
                FROM amazon.dim_user AS u WHERE s.user_id = u.user_id AND
                s.user_name = u.user_name'''
                cursor.execute(user_key)

                review_key = '''UPDATE stg_amazon_sales_report AS s SET review_key = rr.review_key 
                FROM amazon.dim_review AS rr WHERE s.review_id = rr.review_id AND
                s.review_title = rr.review_content'''
                cursor.execute(review_key)

                # Loading dim_product table's surrogate keys from staging to dim_review.

                load_to_dim_review = '''UPDATE amazon.dim_review r SET product_key = sa.product_key
                FROM stg_amazon_sales_report sa
                WHERE r.review_id = sa.review_id'''
                cursor.execute(load_to_dim_review)

                # Had the product_key data not been present on staging but on the dim_product table
                # the script below would've been used.

                # load_to_dim_review = '''UPDATE amazon.dim_review r SET product_key = p.product_key
                # FROM stg_amazon_sales_report sa
                # JOIN amazon.dim_product p ON sa.product_id = p.product_id
                # WHERE r.review_id = sa.review_id'''
                # cursor.execute(load_to_dim_review)

                # Loading surrogate keys from staging table to join table.
                load_to_bridge = '''INSERT INTO amazon.product_user_join (product_key, user_key)
                SELECT product_key, user_key
                FROM stg_amazon_sales_report'''
                cursor.execute(load_to_bridge)

                # Had the product_key and user_key not been present on staging but on the dim_product and
                # dim_user tables the script below would've been used.

                # load_to_bridge = '''INSERT INTO amazon.product_user_join (product_key, user_key)
                # SELECT
                # p.product_key,
                # u.user_key
                # FROM stg_amazon_sales_report sa
                # JOIN amazon.dim_product p ON sa.product_id = p.product_id
                # JOIN amazon.dim_user u ON sa.user_id = u.user_id'''
                # cursor.execute(load_to_bridge)

                return print('All target tables updated with surrogate keys successfully')

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


# Finally, defining the function that transforms and loads data to the fact table together with
# all fetched surrogate keys.

def transform_load_fact_table():
    engine = create_engine('postgresql:///Destination')

    dg = pd.read_sql('stg_amazon_sales_report', engine)
    fact = dg[['discounted_price', 'actual_price', 'discount_percentage',
               'product_key']].copy()

    fact['discounted_price'] = fact['discounted_price'].str.replace('₹', '')
    fact['discounted_price'] = fact['discounted_price'].str.replace(',', '').astype(float)
    fact['actual_price'] = fact['actual_price'].str.replace('₹', '')
    fact['actual_price'] = fact['actual_price'].str.replace(',', '').astype(float)
    fact = fact.drop_duplicates(subset=['product_key'], keep='first')
    fact = fact.to_dict('records')

    insert_query = '''INSERT into amazon.fact_price (
    "actual_price (PLN)",
    "discounted_price (PLN)",
    discount_percentage,
    product_key
    ) 
    VALUES (
    %(actual_price)s,
    %(discounted_price)s,
    %(discount_percentage)s,
    %(product_key)s
    )'''

    insert(insert_query, fact)
    return print('fact_table loaded successfully')


extract_transform()

load_dim_review()

load_dim_user()

load_dim_product()

load_surrogate_keys()

transform_load_fact_table()