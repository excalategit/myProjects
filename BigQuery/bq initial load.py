import pandas as pd
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from sqlalchemy import create_engine
from google.cloud import bigquery
from datetime import datetime
from datetime import timedelta
from time import time

client = bigquery.Client()

try:
    dim_product = '''
    CREATE TABLE IF NOT EXISTS my-dw-project-01.bq_upload.dim_product (
    product_key STRING DEFAULT GENERATE_UUID(),
    product_id STRING,
    product_name STRING,
    category STRING,
    about_product STRING,
    img_link STRING,
    product_link STRING,
    rating STRING,
    rating_count STRING,
    created_date DATE DEFAULT CURRENT_DATE,
    last_updated_date DATE
    )'''
    query_job = client.query(dim_product)
    query_job.result()

    dim_user = '''
    CREATE TABLE IF NOT EXISTS my-dw-project-01.bq_upload.dim_user (
    user_key STRING DEFAULT GENERATE_UUID(),
    user_id STRING,
    user_name STRING,
    created_date DATE DEFAULT CURRENT_DATE,
    last_updated_date DATE
    )'''
    query_job = client.query(dim_user)
    query_job.result()

    dim_review = '''
    CREATE TABLE IF NOT EXISTS my-dw-project-01.bq_upload.dim_review (
    review_key STRING DEFAULT GENERATE_UUID(),
    review_id STRING,
    review_content STRING,
    user_key STRING,
    product_key STRING,
    created_date DATE DEFAULT CURRENT_DATE,
    last_updated_date DATE
    )'''
    query_job = client.query(dim_review)
    query_job.result()

    fact_price = '''
    CREATE TABLE IF NOT EXISTS my-dw-project-01.bq_upload.fact_price (
    price_key STRING DEFAULT GENERATE_UUID(),
    actual_price_PLN FLOAT64,
    discounted_price_PLN FLOAT64,
    discount_percentage STRING,
    product_key STRING,
    created_date DATE DEFAULT CURRENT_DATE
    )'''
    query_job = client.query(fact_price)
    query_job.result()

    etl_audit_log = '''
    CREATE TABLE IF NOT EXISTS my-dw-project-01.bq_upload.etl_audit_log (
    log_id STRING DEFAULT GENERATE_UUID(),
    table_name STRING,
    staging_count INT64,
    insert_count INT64,
    update_count INT64,
    status STRING,
    log_date DATE DEFAULT CURRENT_DATE
    )'''
    query_job = client.query(etl_audit_log)
    query_job.result()

except Exception as error:
    print(error)


def extract_transform():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'stg_bq_project'

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_sql('bq_source_data', engine)
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
        source_table = source_table.explode(['user_id', 'user_name', 'review_id', 'review_title'])

        source_table['created_date'] = datetime.today().date()

        to_gbq(source_table, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='fail')

        # Addition of surrogate key columns to staging.

        staging_update = '''ALTER TABLE my-dw-project-01.bq_upload.stg_bq_project
        ADD COLUMN product_key STRING,
        ADD COLUMN user_key STRING,
        ADD COLUMN review_key STRING
        '''

        query_job = client.query(staging_update)
        query_job.result()

        return print('Extraction to staging completed')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')


def load_dim_product():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'dim_product'
    column_name = 'product_id'

    try:
        dp = read_gbq('my-dw-project-01.bq_upload.stg_bq_project', 'my-dw-project-01')
        product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                      'rating', 'rating_count']].copy()
        product['rating_count'] = product['rating_count'].fillna(1)
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')

        t1 = time()
        to_gbq(product, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='fail')
        t2 = time()

        print(f'Rows 0 to {len(product)} loaded successfully for {table_name} in {t2-t1}s')

        call_procedure = ''' 
        call `my-dw-project-01.bq_upload.audit_table`('my-dw-project-01.bq_upload.dim_product', 'product_id')
        '''

        query_job = client.query(call_procedure)
        query_job.result()

        return print('Audit table updated.')

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')


def load_dim_user():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'dim_user'
    column_name = 'user_id'

    try:
        du = read_gbq('my-dw-project-01.bq_upload.stg_bq_project', 'my-dw-project-01')
        user = du[['user_id', 'user_name']].copy()
        user = user.drop_duplicates()

        t1 = time()
        to_gbq(user, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='fail')
        t2 = time()

        print(f'Rows 0 to {len(user)} loaded successfully for {table_name} in {t2-t1}s')

        call_procedure = ''' 
        call `my-dw-project-01.bq_upload.audit_table`('my-dw-project-01.bq_upload.dim_user', 'user_id')
        '''

        query_job = client.query(call_procedure)
        query_job.result()

        return print('Audit table updated.')

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')


def load_dim_review():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'dim_review'
    column_name = 'review_id'

    try:
        dr = read_gbq('my-dw-project-01.bq_upload.stg_bq_project', 'my-dw-project-01')
        review = dr[['review_id', 'review_title']].copy()
        review = review.rename(columns={'review_title': 'review_content'})
        review = review.drop_duplicates(subset=['review_id'], keep='first')

        t1 = time()
        to_gbq(review, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='fail')
        t2 = time()

        print(f'Rows 0 to {len(review)} loaded successfully for {table_name} in {t2-t1}s')

        call_procedure = ''' 
        call `my-dw-project-01.bq_upload.audit_table`('my-dw-project-01.bq_upload.dim_review', 'review_id')
        '''

        query_job = client.query(call_procedure)
        query_job.result()

        return print('Audit table updated.')

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')


def load_surrogate_keys():

    try:
        # Loading surrogate keys from dimension tables to staging.
        product_key = '''UPDATE my-dw-project-01.bq_upload.stg_bq_project AS s SET product_key = p.product_key
        FROM my-dw-project-01.bq_upload.dim_product AS p WHERE s.product_id = p.product_id AND
        s.product_name = p.product_name'''
        query_job = client.query(product_key)
        query_job.result()

        user_key = '''UPDATE my-dw-project-01.bq_upload.stg_bq_project AS s SET user_key = u.user_key
        FROM my-dw-project-01.bq_upload.dim_user AS u WHERE s.user_id = u.user_id AND
        s.user_name = u.user_name'''
        query_job = client.query(user_key)
        query_job.result()

        review_key = '''UPDATE my-dw-project-01.bq_upload.stg_bq_project AS s SET review_key = r.review_key
        FROM my-dw-project-01.bq_upload.dim_review AS r WHERE s.review_id = r.review_id AND
        s.review_title = r.review_content'''
        query_job = client.query(review_key)
        query_job.result()

        # Loading dim_product table's surrogate keys from staging to dim_review.

        load_prod_review = '''UPDATE my-dw-project-01.bq_upload.dim_review r SET product_key = s.product_key
        FROM my-dw-project-01.bq_upload.stg_bq_project s
        WHERE r.review_id = s.review_id'''
        query_job = client.query(load_prod_review)
        query_job.result()

        # Loading dim_user table's surrogate keys from staging to dim_review.

        load_user_review = '''UPDATE my-dw-project-01.bq_upload.dim_review r SET user_key = s.user_key
        FROM my-dw-project-01.bq_upload.stg_bq_project s
        WHERE r.review_id = s.review_id'''
        query_job = client.query(load_user_review)
        query_job.result()

        return print('All target tables updated with surrogate keys successfully')

    except Exception as error:
        print(f'Loading surrogate keys failed: {error}')


def transform_load_fact_table():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'fact_price'
    column_name = 'product_key'

    try:
        dg = read_gbq('bq_upload.stg_bq_project', 'my-dw-project-01')
        fact = dg[['discounted_price', 'actual_price', 'discount_percentage',
                   'product_key']].copy()

        fact['discounted_price'] = fact['discounted_price'].str.replace('₹', '')
        fact['discounted_price'] = fact['discounted_price'].str.replace(',', '').astype(float)
        fact['actual_price'] = fact['actual_price'].str.replace('₹', '')
        fact['actual_price'] = fact['actual_price'].str.replace(',', '').astype(float)

        fact = fact.rename(columns={'discounted_price': 'discounted_price_pln'})
        fact = fact.rename(columns={'actual_price': 'actual_price_pln'})
        fact = fact.drop_duplicates(subset=['product_key'], keep='first')

        t1 = time()
        to_gbq(fact, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='fail')
        t2 = time()

        print(f'Rows 0 to {len(fact)} loaded successfully for {table_name} in {t2-t1}s')

        call_procedure = ''' 
        call `my-dw-project-01.bq_upload.audit_table`('my-dw-project-01.bq_upload.fact_price', 'product_key')
        '''

        query_job = client.query(call_procedure)
        query_job.result()

        return print('Audit table updated.')

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')


extract_transform()

load_dim_review()

load_dim_user()

load_dim_product()

load_surrogate_keys()

transform_load_fact_table()