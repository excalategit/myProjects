import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

# Creating the fact, dimension and audit tables.

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
            options='-c search_path=ebay') as connection:

        with connection.cursor() as cursor:
            create_dim_product = '''
            CREATE TABLE IF NOT EXISTS dim_product (
            product_key SERIAL PRIMARY KEY,
            product_id TEXT UNIQUE,
            product_name TEXT,
            category TEXT,
            about_product TEXT,
            img_link TEXT,
            product_link TEXT,
            rating TEXT,
            rating_count TEXT,
            created_date DATE DEFAULT CURRENT_DATE,
            last_updated_date DATE
            )'''

            cursor.execute(create_dim_product)

            create_dim_user = '''
            CREATE TABLE IF NOT EXISTS dim_user (
            user_key SERIAL PRIMARY KEY,
            user_id TEXT UNIQUE,
            user_name TEXT,
            created_date DATE DEFAULT CURRENT_DATE,
            last_updated_date DATE
            )'''

            cursor.execute(create_dim_user)

            create_dim_review = '''
            CREATE TABLE IF NOT EXISTS dim_review (
            review_key SERIAL PRIMARY KEY,
            review_id TEXT UNIQUE,
            review_content TEXT,
            user_key INT REFERENCES dim_user (user_key),
            product_key INT REFERENCES dim_product (product_key),
            created_date DATE DEFAULT CURRENT_DATE,
            last_updated_date DATE
            )'''

            cursor.execute(create_dim_review)

            create_fact_price = '''
            CREATE TABLE IF NOT EXISTS fact_price (
            price_key SERIAL PRIMARY KEY,
            "actual_price (PLN)" FLOAT,
            "discounted_price (PLN)" FLOAT,
            discount_percentage TEXT,
            product_key INT REFERENCES dim_product (product_key),
            created_date DATE DEFAULT CURRENT_DATE
            )'''

            cursor.execute(create_fact_price)

            create_etl_audit_table = '''
            CREATE TABLE IF NOT EXISTS etl_audit_log (
            log_id SERIAL PRIMARY KEY,
            table_name TEXT,
            staging_count INT,
            insert_count INT,
            update_count INT,
            status TEXT,
            log_date DATE DEFAULT CURRENT_DATE
            )'''

            cursor.execute(create_etl_audit_table)

            # Creating the stored procedure that updates the audit table when called. To derive the number
            # of rows updated simply use the same condition defined at the ON CONFLICT step of UPSERT
            # in this case setting last_updated_date column to current date.
            create_procedure = '''
            CREATE OR REPLACE PROCEDURE audit_table(p_table_name text, p_column_name text)
            LANGUAGE plpgsql
            AS $$
            DECLARE
                v_staging_count INT;
                v_insert_count INT;
                v_update_count INT;
                v_result TEXT;

            BEGIN
                EXECUTE format ('SELECT COUNT (DISTINCT %I) FROM public.stg_product_review
                WHERE created_date = CURRENT_DATE', p_column_name) INTO v_staging_count;

                IF p_table_name != 'fact_price' THEN
                    EXECUTE format ('SELECT COUNT(*) FROM %I
                    WHERE created_date = CURRENT_DATE AND last_updated_date IS NULL', p_table_name) 
                    INTO v_insert_count;
                
                    EXECUTE format ('SELECT COUNT(*) FROM %I
                    WHERE last_updated_date = CURRENT_DATE', p_table_name) 
                    INTO v_update_count;
                    
                ELSE
                    v_update_count = 0;
                    
                    EXECUTE format ('SELECT COUNT(*) FROM %I
                    WHERE created_date = CURRENT_DATE', p_table_name) 
                    INTO v_insert_count;
                    
                END IF;

                IF v_staging_count = v_insert_count + v_update_count THEN
                    v_result := 'PASS';
                ELSE
                    v_result := 'FAIL';
                END IF;

                INSERT INTO etl_audit_log (table_name, staging_count, insert_count, update_count, status)
                VALUES (p_table_name, v_staging_count, v_insert_count, v_update_count, v_result);
                
            END $$
            '''
            cursor.execute(create_procedure)

except Exception as error:
    print(error)

finally:
    if connection is not None:
        connection.close()


# Defining the function that performs the INSERT action when called by the ETL stages.
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
                psycopg2.extras.execute_batch(cursor, insert_query, dataset)
                print(f'Rows {loaded_rows} to {len(dataset)} loaded successfully for {table_name}')
                loaded_rows += len(dataset)

                call_procedure = ''' 
                call audit_table(%s, %s)
                '''

                cursor.execute(call_procedure, (table_name, column_name,))
                # Reminder that arguments are passed to placeholders in psycopg2 using
                # tuples or lists, even if it is only one value.
                print('Audit table updated.')

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')

    finally:
        if connection is not None:
            connection.close()


# Defining the function that extracts and transforms source data to staging.
def extract_transform():
    connection = None

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_excel('incremental load/ebay.xlsx')
        source_table = source_table.copy()
        source_table['modified_date'] = pd.to_datetime(source_table['modified_date']).dt.date
        source_table = source_table[source_table['modified_date'] == datetime.today().date() - timedelta(days=1)]
        # This design fetches data from any past 'modified date' in the source data, in this case,
        # data modified yesterday, but in reality it should fetch all data from the source system
        # including historical data since it is a first load.

        source_table['user_id'] = source_table['user_id'].str.split(',')
        source_table['user_name'] = source_table['user_name'].str.split(',')
        source_table['review_id'] = source_table['review_id'].str.split(',')
        source_table['review_title'] = source_table['review_title'].str.split(',')
        source_table['created_date'] = datetime.today().date()

        source_table = source_table.explode(['user_id', 'user_name', 'review_id', 'review_title'])
        source_table.to_sql('stg_product_review', engine, index=False, if_exists='fail')

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
                staging_update = '''ALTER TABLE stg_product_review
                ADD COLUMN product_key INT,
                ADD COLUMN user_key INT,
                ADD COLUMN review_key INT
                '''

                cursor.execute(staging_update)

                return print('Extraction to staging completed')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')

    finally:
        if connection is not None:
            connection.close()


# Defining the functions that loads the transformed data to the dimension tables and updates the audit table.
def load_dim_product():
    table_name = 'dim_product'
    column_name = 'product_id'

    try:
        engine = create_engine('postgresql:///Destination')

        dp = pd.read_sql('stg_product_review', engine)
        product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                      'rating', 'rating_count']].copy()
        product['rating_count'] = product['rating_count'].fillna(1)
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')
        # It is best practice to deduplicate dim tables using business keys alone.
        product = product.to_dict('records')

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
        )'''

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
        user = du[['user_id', 'user_name']].copy()
        user = user.drop_duplicates()
        user = user.to_dict('records')

        insert_query = '''INSERT into dim_user (
        user_id,
        user_name
        ) 
        VALUES (
        %(user_id)s,
        %(user_name)s
        )'''

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
        review = drr[['review_id', 'review_title']].copy()
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
        )'''

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
                # Loading surrogate keys from dimension tables to staging.

                product_key = '''UPDATE stg_product_review AS s SET product_key = p.product_key
                FROM ebay.dim_product AS p WHERE s.product_id = p.product_id AND
                s.product_name = p.product_name'''
                cursor.execute(product_key)

                user_key = '''UPDATE stg_product_review AS s SET user_key = u.user_key
                FROM ebay.dim_user AS u WHERE s.user_id = u.user_id AND
                s.user_name = u.user_name'''
                cursor.execute(user_key)

                review_key = '''UPDATE stg_product_review AS s SET review_key = r.review_key
                FROM ebay.dim_review AS r WHERE s.review_id = r.review_id AND
                s.review_title = r.review_content'''
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
        print(f'Loading surrogate keys failed: {error}')

    finally:
        if connection is not None:
            connection.close()


# Defining the function that transforms and loads data from staging to the fact table
# together with all surrogate keys.
def transform_load_fact_table():
    table_name = 'fact_price'
    column_name = 'product_key'

    try:
        engine = create_engine('postgresql:///Destination')

        dg = pd.read_sql('stg_product_review', engine)
        fact = dg[['discounted_price', 'actual_price', 'discount_percentage',
                   'product_key']].copy()

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

load_dim_review()

load_dim_user()

load_dim_product()

load_surrogate_keys()

transform_load_fact_table()