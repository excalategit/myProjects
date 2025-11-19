import pandas as pd
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from sqlalchemy import create_engine
from google.cloud import bigquery
from datetime import datetime
from datetime import timedelta
from time import time

client = bigquery.Client()

# Defining the function that extracts and transforms source data to staging.
def extract_transform():
    try:
        engine = create_engine('postgresql:///Destination')

        # This design fetches data from any past 'modified date' in the source data, in this case,
        # data modified yesterday, but in reality it should fetch all data from the source system
        # including historical data since it is a first load.
        source_table = pd.read_sql('bq_source_data', engine)
        source_table = source_table.copy()
        source_table['modified_date'] = pd.to_datetime(source_table['modified_date']).dt.date
        source_table = source_table[source_table['modified_date'] == datetime.today().date() - timedelta(days=1)]

        # Note that all required data transformations are completed before loading to staging.
        source_table['user_id'] = source_table['user_id'].str.split(',')
        source_table['user_name'] = source_table['user_name'].str.split(',')
        source_table['review_id'] = source_table['review_id'].str.split(',')
        source_table['review_title'] = source_table['review_title'].str.split(',')
        source_table = source_table.explode(['user_id', 'user_name', 'review_id', 'review_title'])

        source_table['rating_count'] = source_table['rating_count'].fillna(1)
        source_table['discounted_price'] = source_table['discounted_price'].str.replace('₹', '')
        source_table['discounted_price'] = source_table['discounted_price'].str.replace(',', '').astype(float)
        source_table['actual_price'] = source_table['actual_price'].str.replace('₹', '')
        source_table['actual_price'] = source_table['actual_price'].str.replace(',', '').astype(float)

        # source_table = source_table.rename(columns={'review_title': 'review_content'})
        # source_table = source_table.rename(columns={'discounted_price': 'discounted_price_pln'})
        # source_table = source_table.rename(columns={'actual_price': 'actual_price_pln'})

        source_table = source_table.drop_duplicates()

        # Finally, the data is assigned a created date of 'today' for audit purposes before loading to staging
        source_table['created_date'] = datetime.today().date()

        to_gbq(source_table, 'my-dw-project-01.bq_upload.stg_bq_project',
               project_id='my-dw-project-01', if_exists='fail')

        # Addition of surrogate key columns to staging.
        staging_update = '''ALTER TABLE my-dw-project-01.bq_upload.stg_bq_project
        ADD COLUMN product_key STRING,
        ADD COLUMN user_key STRING
        '''

        query_job = client.query(staging_update)
        query_job.result()

        return print('Extraction to staging completed')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')


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
    review_title STRING,
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
    actual_price FLOAT64,
    discounted_price FLOAT64,
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
    load_time NUMERIC,
    log_date DATE DEFAULT CURRENT_DATE
    )'''
    query_job = client.query(etl_audit_log)
    query_job.result()

except Exception as error:
    print(error)


# Defining the function that loads the data and updates the audit table when called.
def loader(project_id, dataset_id, dataframe, table_name, table_name_bq, column_name):
    try:
        t1 = time()
        to_gbq(dataframe, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='fail')
        t2 = time()

        load_time = t2-t1

        print(f'Rows 0 to {len(dataframe)} loaded successfully for {table_name} in {load_time}s')

        try:
            call_procedure = ''' 
                    call `my-dw-project-01.bq_upload.audit_table`(@table_name_bq, @column_name, @load_time)
                    '''

            # job_config enables arguments to be supplied to a function that is being called
            # through the BigQuery connection engine, in this case the function is the stored procedure.
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter('table_name_bq', 'STRING', table_name_bq),
                    bigquery.ScalarQueryParameter('column_name', 'STRING', column_name),
                    bigquery.ScalarQueryParameter('load_time', 'NUMERIC', load_time)
                ],
            )
            query_job = client.query(call_procedure, job_config)
            query_job.result()

            print('Audit table updated.')

        except Exception as error:
            print(f'Loading failed for audit table: {error}')

    except Exception as error:
        print(f'Loading failed for {table_name}: {error}')


# Defining the functions that specify the loading for each of the tables.
def load_dim_product():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'dim_product'
    table_name_bq = 'my-dw-project-01.bq_upload.dim_product'
    column_name = 'product_id'

    try:
        dp = read_gbq('my-dw-project-01.bq_upload.stg_bq_project', 'my-dw-project-01')
        product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                      'rating', 'rating_count']].copy()
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')

        loader(project_id, dataset_id, product, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Transformation stage failed for {table_name}: {error}')


def load_dim_user():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'dim_user'
    table_name_bq = 'my-dw-project-01.bq_upload.dim_user'
    column_name = 'user_id'

    try:
        du = read_gbq('my-dw-project-01.bq_upload.stg_bq_project', 'my-dw-project-01')
        user = du[['user_id', 'user_name']].copy()
        user = user.drop_duplicates()

        loader(project_id, dataset_id, user, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Transformation stage failed for {table_name}: {error}')


def load_dim_review():
    project_id = 'my-dw-project-01'
    dataset_id = 'bq_upload'
    table_name = 'dim_review'
    table_name_bq = 'my-dw-project-01.bq_upload.dim_review'
    column_name = 'review_id'

    try:
        dr = read_gbq('my-dw-project-01.bq_upload.stg_bq_project', 'my-dw-project-01')
        review = dr[['review_id', 'review_title']].copy()
        review = review.drop_duplicates(subset=['review_id'], keep='first')

        loader(project_id, dataset_id, review, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Transformation stage failed for {table_name}: {error}')


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
    table_name_bq = 'my-dw-project-01.bq_upload.fact_price'
    column_name = 'product_key'

    try:
        dg = read_gbq('bq_upload.stg_bq_project', 'my-dw-project-01')
        fact = dg[['discounted_price', 'actual_price', 'discount_percentage',
                   'product_key']].copy()
        fact = fact.drop_duplicates(subset=['product_key'], keep='first')

        loader(project_id, dataset_id, fact, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Transformation stage failed for {table_name}: {error}')


extract_transform()

load_dim_product()

load_dim_user()

load_dim_review()

load_surrogate_keys()

transform_load_fact_table()