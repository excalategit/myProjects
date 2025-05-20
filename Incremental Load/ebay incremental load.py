import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()


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


# Defining the function that extracts and transforms source data to staging.

def extract_transform():

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_excel('incremental load/ebay.xlsx')
        source_table = source_table.copy()
        source_table['modified_date'] = pd.to_datetime(source_table['modified_date']).dt.date
        source_table = source_table[source_table['modified_date'] == datetime.today().date() - timedelta(days=1)]
        source_table['user_id'] = source_table['user_id'].str.split(',')
        source_table['user_name'] = source_table['user_name'].str.split(',')
        source_table['review_id'] = source_table['review_id'].str.split(',')
        source_table['review_title'] = source_table['review_title'].str.split(',')
        source_table = source_table.explode(['user_id', 'user_name', 'review_id', 'review_title'])
        source_table['created_date'] = datetime.today().date()
        source_table.to_sql('stg_product_review', engine, index=False, if_exists='append')

        return print('Extraction to staging completed')

    except Exception as error:
        print(error)


# Defining the function that loads the transformed data to the dimension tables.

def load_dim_product():
    engine = create_engine('postgresql:///Destination')

    dp = pd.read_sql('stg_product_review', engine)
    product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                  'rating', 'rating_count', 'created_date']].copy()
    product['created_date'] = pd.to_datetime(product['created_date']).dt.date
    product = product[product['created_date'] == datetime.today().date()]
    product['rating_count'] = product['rating_count'].fillna(1)
    product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')
    # It is best practice to deduplicate dim tables using business keys alone.
    product = product.to_dict('records')

    insert_query = '''INSERT into ebay.dim_product (
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
    )
    ON CONFLICT (product_id)
    DO UPDATE SET
    product_name = excluded.product_name,
    category = excluded.category,
    about_product = excluded.about_product,
    img_link = excluded.img_link,
    product_link = excluded.product_link,
    rating = excluded.rating,
    rating_count = excluded.rating_count
    '''

    insert(insert_query, product)
    return print('dim_product loaded successfully')


def load_dim_user():
    engine = create_engine('postgresql:///Destination')

    du = pd.read_sql('stg_product_review', engine)
    user = du[['user_id', 'user_name', 'created_date']].copy()
    user['created_date'] = pd.to_datetime(user['created_date']).dt.date
    user = user[user['created_date'] == datetime.today().date()]
    user = user.drop_duplicates()
    user = user.to_dict('records')

    insert_query = '''INSERT into ebay.dim_user (
    user_id,
    user_name
    ) 
    VALUES (
    %(user_id)s,
    %(user_name)s
    )
    ON CONFLICT (user_id)
    DO UPDATE SET
    user_name = excluded.user_name
    '''

    insert(insert_query, user)
    return print('dim_user loaded successfully')


def load_dim_review():
    engine = create_engine('postgresql:///Destination')

    drr = pd.read_sql('stg_product_review', engine)
    review = drr[['review_id', 'review_title', 'created_date']].copy()
    review['created_date'] = pd.to_datetime(review['created_date']).dt.date
    review = review[review['created_date'] == datetime.today().date()]
    review = review.rename(columns={'review_title': 'review_content'})
    review = review.drop_duplicates(subset=['review_id'], keep='first')
    review = review.to_dict('records')

    insert_query = '''INSERT into ebay.dim_review (
    review_id,
    review_content
    )
    VALUES (
    %(review_id)s,
    %(review_content)s
    )
    ON CONFLICT (review_id)
    DO UPDATE SET
    review_content = excluded.review_content
    '''

    insert(insert_query, review)
    return print('dim_review loaded successfully')


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
                # Loading surrogate keys from dimension tables to staging.

                product_key = '''UPDATE stg_product_review AS s SET product_key = p.product_key 
                FROM ebay.dim_product AS p WHERE s.created_date = CURRENT_DATE AND 
                s.product_id = p.product_id AND s.product_name = p.product_name'''
                cursor.execute(product_key)

                user_key = '''UPDATE stg_product_review AS s SET user_key = u.user_key 
                FROM ebay.dim_user AS u WHERE s.created_date = CURRENT_DATE AND 
                s.user_id = u.user_id AND s.user_name = u.user_name'''
                cursor.execute(user_key)

                review_key = '''UPDATE stg_product_review AS s SET review_key = r.review_key 
                FROM ebay.dim_review AS r WHERE s.created_date = CURRENT_DATE AND 
                s.review_id = r.review_id AND s.review_title = r.review_content'''
                cursor.execute(review_key)

                # Loading dim_product table's surrogate keys from staging to dim_review.

                load_prod_review = '''UPDATE ebay.dim_review r SET product_key = sa.product_key
                FROM stg_product_review sa
                WHERE r.review_id = sa.review_id'''
                cursor.execute(load_prod_review)

                # Loading dim_user table's surrogate keys from staging to dim_review.

                load_user_review = '''UPDATE ebay.dim_review r SET user_key = sa.user_key
                FROM stg_product_review sa
                WHERE r.review_id = sa.review_id'''
                cursor.execute(load_user_review)

                return print('All target tables updated with surrogate keys successfully')

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


# Finally, defining the function that transforms and loads data from staging to the fact table together with
# all surrogate keys.

def transform_load_fact_table():
    engine = create_engine('postgresql:///Destination')

    dg = pd.read_sql('stg_product_review', engine)
    fact = dg[['discounted_price', 'actual_price', 'discount_percentage',
               'product_key', 'created_date']].copy()

    fact['created_date'] = pd.to_datetime(fact['created_date']).dt.date
    fact = fact[fact['created_date'] == datetime.today().date()]
    fact['discounted_price'] = fact['discounted_price'].str.replace('₹', '')
    fact['discounted_price'] = fact['discounted_price'].str.replace(',', '').astype(float)
    fact['actual_price'] = fact['actual_price'].str.replace('₹', '')
    fact['actual_price'] = fact['actual_price'].str.replace(',', '').astype(float)
    fact = fact.drop_duplicates(subset=['product_key'], keep='first')
    fact = fact.to_dict('records')

    insert_query = '''INSERT into ebay.fact_price (
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