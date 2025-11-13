import pandas as pd
from sqlalchemy import create_engine
from pandas_gbq import to_gbq
from google.cloud import bigquery
from datetime import datetime
from datetime import timedelta
from time import time

client = bigquery.Client()

def extract_transform():
    project_id = 'my-dw-project-01'

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_sql('bq_source_data', engine)
        source_table = source_table.copy()

        source_table['modified_date'] = pd.to_datetime(source_table['modified_date']).dt.date
        source_table = source_table[source_table['modified_date'] == datetime.today().date() - timedelta(days=0)]


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

        source_table = source_table.drop_duplicates()

        to_gbq(source_table, 'my-dw-project-01.bq_upload_test.stg_bq_test', project_id=project_id, if_exists='append')

        return print('Extraction to staging completed.')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')


def load_incremental():
    table_name = 'dim_product'
    table_name_bq = 'my-dw-project-01.bq_upload_test.dim_product'
    column_name = 'product_id'

    # SELECT * EXCEPT(row_num) FROM (
    #     SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_date DESC) AS row_num
    #     FROM `my-dw-project-01.bq_upload_test.stg_bq_test`
    #     WHERE modified_date = (select max(modified_date) from `my-dw-project-01.bq_upload_test.stg_bq_test`)
    # ) WHERE row_num = 1) s

    try:
        insert_query = """
        MERGE `my-dw-project-01.bq_upload_test.dim_product` p
        USING (
            SELECT * FROM `my-dw-project-01.bq_upload_test.stg_bq_test`
            WHERE modified_date = (select max(modified_date) from `my-dw-project-01.bq_upload_test.stg_bq_test`)
            )
        ON p.product_id = s.product_id
        WHEN MATCHED THEN
          UPDATE SET p.product_name = s.product_name, p.category = s.category, p.about_product = s.about_product,
          p.img_link = s.img_link, p.product_link = s.product_link, p.rating = s.rating, 
          p.rating_count = s.rating_count, p.created_date = s.created_date, p.last_updated_date = s.created_date
        WHEN NOT MATCHED THEN
          INSERT (product_id, product_name, category, about_product, img_link, product_link,
          rating, rating_count, created_date)
          VALUES (s.product_id, s.product_name, s.category, s.about_product, s.img_link, s.product_link,
          s.rating, s.rating_count, s.created_date)
        """

        try:
            t1 = time()
            query_job = client.query(insert_query)
            query_job.result()
            t2 = time()

            load_time = t2 - t1

            print(f'Rows loaded successfully for {table_name} in {load_time}s')

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
        print(f'Potential issue with transformation step: {error}')


extract_transform()

load_incremental()
