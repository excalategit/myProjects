import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv
from time import time

load_dotenv()

# Defining the function that extracts and transforms source data to staging.

def extract_transform():
    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_excel('incremental load/ebay.xlsx')
        source_table = source_table.copy()

        # This design allows customization of modified date e.g. allowing only data
        # modified yesterday to be fetched (mirroring an incremental loading design).
        source_table['modified_date'] = pd.to_datetime(source_table['modified_date']).dt.date
        source_table = source_table[source_table['modified_date'] == datetime.today().date() - timedelta(days=1)]

        source_table['user_id'] = source_table['user_id'].str.split(',')
        source_table['user_name'] = source_table['user_name'].str.split(',')
        source_table['review_id'] = source_table['review_id'].str.split(',')
        source_table['review_title'] = source_table['review_title'].str.split(',')
        source_table['created_date'] = datetime.today().date()

        source_table = source_table.explode(['user_id', 'user_name', 'review_id', 'review_title'])
        source_table.to_sql('stg_product_review', engine, index=False, if_exists='append')

        return print('Extraction to staging completed.')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')


# Defining the function that loads the data and updates the audit table when called.

def insert(insert_query, dataset, table_name, column_name):
    connection = None
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    loaded_rows = 0

    try:
        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432,
                options='-c search_path=ebay') as connection:

            with connection.cursor() as cursor:
                t1 = time()
                psycopg2.extras.execute_batch(cursor, insert_query, dataset)
                t2 = time()
                print(f'Rows {loaded_rows} to {len(dataset)} loaded successfully for {table_name} in {t2-t1}s')
                loaded_rows += len(dataset)

                call_procedure = ''' 
                call audit_table(%s, %s)
                '''

                cursor.execute(call_procedure, (table_name, column_name,))
                print('Audit table updated.')

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')

    finally:
        if connection is not None:
            connection.close()


# Defining the functions that specifies the loading for each of the tables.

def load_dim_product():
    table_name = 'dim_product'
    column_name = 'product_id'

    try:
        engine = create_engine('postgresql:///Destination')

        dp = pd.read_sql('stg_product_review', engine)
        product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                      'rating', 'rating_count', 'created_date']].copy()

        # Fetch only today's product data from staging.
        product['created_date'] = pd.to_datetime(product['created_date']).dt.date
        product = product[product['created_date'] == datetime.today().date()]

        product['rating_count'] = product['rating_count'].fillna(1)
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')
        product = product.to_dict('records')

        # UPSERT is used in loading the data. If any update happens, the last_modified_date
        # column is populated with today's date (or in other implementations, a time stamp)
        insert_query = '''INSERT into dim_product (
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
        DO UPDATE 
        SET product_name = excluded.product_name,
            category = excluded.category,
            about_product = excluded.about_product,
            img_link = excluded.img_link,
            product_link = excluded.product_link,
            rating = excluded.rating,
            rating_count = excluded.rating_count,
            last_updated_date = CURRENT_DATE
        '''

        insert(insert_query, product, table_name, column_name)
        return None

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


def load_dim_user():
    table_name = 'dim_user'
    column_name = 'user_id'

    try:
        engine = create_engine('postgresql:///Destination')

        du = pd.read_sql('stg_product_review', engine)
        user = du[['user_id', 'user_name', 'created_date']].copy()

        # Fetch only today's user data from staging.
        user['created_date'] = pd.to_datetime(user['created_date']).dt.date
        user = user[user['created_date'] == datetime.today().date()]

        user = user.drop_duplicates()
        user = user.to_dict('records')

        insert_query = '''INSERT into dim_user (
        user_id,
        user_name
        ) 
        VALUES (
        %(user_id)s,
        %(user_name)s
        )
        ON CONFLICT (user_id)
        DO UPDATE 
        SET user_name = excluded.user_name,
            last_updated_date = CURRENT_DATE
        '''

        insert(insert_query, user, table_name, column_name)
        return None

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


def load_dim_review():
    table_name = 'dim_review'
    column_name = 'review_id'

    try:
        engine = create_engine('postgresql:///Destination')

        drr = pd.read_sql('stg_product_review', engine)
        review = drr[['review_id', 'review_title', 'created_date']].copy()

        # Fetch only today's review data from staging.
        review['created_date'] = pd.to_datetime(review['created_date']).dt.date
        review = review[review['created_date'] == datetime.today().date()]

        review = review.rename(columns={'review_title': 'review_content'})
        review = review.drop_duplicates(subset=['review_id'], keep='first')
        review = review.to_dict('records')

        insert_query = '''INSERT into dim_review (
        review_id,
        review_content
        )
        VALUES (
        %(review_id)s,
        %(review_content)s
        )
        ON CONFLICT (review_id)
        DO UPDATE 
        SET review_content = excluded.review_content,
            last_updated_date = CURRENT_DATE
        '''

        insert(insert_query, review, table_name, column_name)
        return None

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


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
                # Loading surrogate keys from dimension tables to staging. For efficiency,
                # only today's newly generated surrogate keys are added to staging.

                product_key = '''UPDATE stg_product_review AS s SET product_key = p.product_key 
                FROM ebay.dim_product AS p WHERE s.created_date = CURRENT_DATE AND 
                s.product_id = p.product_id AND s.product_name = p.product_name'''
                cursor.execute(product_key)

                user_key = '''UPDATE stg_product_review AS s SET user_key = u.user_key 
                FROM ebay.dim_user AS u WHERE s.created_date = CURRENT_DATE AND 
                s.user_id = u.user_id AND s.user_name = u.user_name'''
                cursor.execute(user_key)

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

                return print('All target tables updated with surrogate keys successfully.')

    except Exception as error:
        print(f'Loading surrogate keys failed: {error}')

    finally:
        if connection is not None:
            connection.close()


# Defining the function that transforms and loads data from staging to the fact table
# together with all surrogate keys.
# Note that the fact table does not require UPSERT, only INSERT.

def transform_load_fact_table():
    table_name = 'fact_price'
    column_name = 'product_key'

    try:
        engine = create_engine('postgresql:///Destination')

        dg = pd.read_sql('stg_product_review', engine)
        fact = dg[['discounted_price', 'actual_price', 'discount_percentage',
                   'product_key', 'created_date']].copy()

        # Fetch only today's fact data from staging.
        fact['created_date'] = pd.to_datetime(fact['created_date']).dt.date
        fact = fact[fact['created_date'] == datetime.today().date()]

        fact['discounted_price'] = fact['discounted_price'].str.replace('₹', '')
        fact['discounted_price'] = fact['discounted_price'].str.replace(',', '').astype(float)
        fact['actual_price'] = fact['actual_price'].str.replace('₹', '')
        fact['actual_price'] = fact['actual_price'].str.replace(',', '').astype(float)
        fact = fact.drop_duplicates(subset=['product_key'], keep='first')
        fact = fact.to_dict('records')

        insert_query = '''INSERT into fact_price (
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

        insert(insert_query, fact, table_name, column_name)
        return None

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


extract_transform()

load_dim_product()

load_dim_user()

load_dim_review()

load_surrogate_keys()

transform_load_fact_table()