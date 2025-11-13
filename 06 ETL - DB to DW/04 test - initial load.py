import pandas as pd
from sqlalchemy import create_engine
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from google.cloud import bigquery
from datetime import datetime
from datetime import timedelta
from time import time

client = bigquery.Client()


# Defining the function that extracts and transforms source data to staging.

def extract_transform():
    project_id = 'my-dw-project-01'

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_sql('bq_source_data', engine)
        source_table = source_table.copy()

        # This design allows the customization of the initial load date based on the modified date
        # column in the source dataset.
        source_table['modified_date'] = pd.to_datetime(source_table['modified_date']).dt.date
        source_table = source_table[source_table['modified_date'] == datetime.today().date() - timedelta(days=6)]

        # Note that all required data transformations are completed before loading to staging
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

        source_table['created_date'] = source_table['modified_date'] + timedelta(days=1)

        # Loading to BigQuery for staging
        to_gbq(source_table, 'my-dw-project-01.bq_upload_test.stg_bq_test', project_id=project_id, if_exists='fail')

        return print('Extraction to staging completed.')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')


def load_initial():
    project_id = 'my-dw-project-01'
    table_name = 'dim_product'
    table_name_bq = 'my-dw-project-01.bq_upload_test.dim_product'
    column_name = 'product_id'

    try:
        dp = read_gbq('my-dw-project-01.bq_upload_test.stg_bq_test', 'my-dw-project-01')
        product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                      'rating', 'rating_count', 'created_date']].copy()
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')

        try:
            t1 = time()
            to_gbq(product, 'my-dw-project-01.bq_upload_test.dim_product', project_id=project_id, if_exists='fail')
            t2 = time()

            load_time = t2 - t1

            print(f'Rows 0 to {len(product)} loaded successfully for {table_name} in {load_time}s')

            try:
                call_procedure = ''' 
                        call `my-dw-project-01.bq_upload_test.audit_table_test`(@table_name_bq, @column_name, @load_time)
                        '''

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

    except Exception as error:
        print(f'Transformation stage failed for {table_name}: {error}')


extract_transform()

load_initial()
