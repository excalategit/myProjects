import pandas as pd
from sqlalchemy import create_engine
from pandas_gbq import to_gbq
from google.cloud import bigquery
from datetime import datetime
from datetime import timedelta
from time import time

client = bigquery.Client()


# Defining the function that extracts and transforms source data to staging.
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
        # This design allows customization of modified date e.g. allowing only data
        # modified yesterday to be fetched (incremental).

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

        source_table['created_date'] = datetime.today().date()

        to_gbq(source_table, f'{dataset_id}.{table_name}', project_id=project_id, if_exists='append')

        return print('Extraction to staging completed.')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')


# Defining the function that will perform the INSERT action when called by the ETL stages.
def insert(insert_query, table_name, table_name_bq, column_name):
    try:
        t1 = time()
        query_job = client.query(insert_query)
        query_job.result()
        t2 = time()

        load_time = t2-t1

        print(f'Rows loaded successfully for {table_name} in {load_time}s')

        try:
            call_procedure = ''' 
                    call `my-dw-project-01.bq_upload.audit_table`(@table_name_bq, @column_name, @load_time)
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


# Defining the functions that loads the transformed data to the dimension tables and updates the audit table.
def load_dim_product():
    table_name = 'dim_product'
    table_name_bq = 'my-dw-project-01.bq_upload.dim_product'
    column_name = 'product_id'

    try:
        insert_query = """
        MERGE `my-dw-project-01.bq_upload.dim_product` p
        USING (
            SELECT * EXCEPT(row_num) FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_date DESC) AS row_num
                FROM `my-dw-project-01.bq_upload.stg_bq_project`
                WHERE created_date = CURRENT_DATE
            )
        WHERE row_num = 1) s
        ON p.product_id = s.product_id
        WHEN MATCHED THEN
          UPDATE SET p.product_name = s.product_name, p.category = s.category, p.about_product = s.about_product,
          p.img_link = s.img_link, p.product_link = s.product_link, p.rating = s.rating, 
          p.rating_count = s.rating_count, p.last_updated_date = CURRENT_DATE
        WHEN NOT MATCHED THEN
          INSERT (product_id, product_name, category, about_product, img_link, product_link,
          rating, rating_count)
          VALUES (s.product_id, s.product_name, s.category, s.about_product, s.img_link, s.product_link,
          s.rating, s.rating_count)
        """
        # The window function used here groups staging data by product_id, orders each group by created_date,
        # and assigns row numbers for each group. Next the top-most product_id of each group is selected.
        # This ensures that unique and latest incarnations of product_ids are selected ready for merge (upsert).

        insert(insert_query, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


def load_dim_user():
    table_name = 'dim_user'
    table_name_bq = 'my-dw-project-01.bq_upload.dim_user'
    column_name = 'user_id'

    try:
        insert_query = """
            MERGE `my-dw-project-01.bq_upload.dim_user` u
            USING (
                SELECT * EXCEPT(row_num) FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_date DESC) AS row_num
                    FROM `my-dw-project-01.bq_upload.stg_bq_project`
                    WHERE created_date = CURRENT_DATE
                )
            WHERE row_num = 1) s
            ON u.user_id = s.user_id
            WHEN MATCHED THEN
              UPDATE SET u.user_name = s.user_name, u.last_updated_date = CURRENT_DATE
            WHEN NOT MATCHED THEN
              INSERT (user_id, user_name)
              VALUES (s.user_id, s.user_name)
            """

        insert(insert_query, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


def load_dim_review():
    table_name = 'dim_review'
    table_name_bq = 'my-dw-project-01.bq_upload.dim_review'
    column_name = 'review_id'

    try:
        insert_query = """
                MERGE `my-dw-project-01.bq_upload.dim_review` r
                USING (
                    SELECT * EXCEPT(row_num) FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY created_date DESC) AS row_num
                        FROM `my-dw-project-01.bq_upload.stg_bq_project`
                        WHERE created_date = CURRENT_DATE
                    )
                WHERE row_num = 1) s
                ON r.review_id = s.review_id
                WHEN MATCHED THEN
                  UPDATE SET r.review_title = s.review_title, r.last_updated_date = CURRENT_DATE
                WHEN NOT MATCHED THEN
                  INSERT (review_id, review_title)
                  VALUES (s.review_id, s.review_title)
                """

        insert(insert_query, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


# Defining the function that fetches and loads surrogate keys to their respective target tables.

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


# Defining the function that transforms and loads data from staging to the fact table
# together with all surrogate keys.

def transform_load_fact_table():
    table_name = 'fact_price'
    table_name_bq = 'my-dw-project-01.bq_upload.fact_price'
    column_name = 'product_key'

    try:
        insert_query = """
                    MERGE `my-dw-project-01.bq_upload.fact_price` f
                    USING (
                        SELECT * EXCEPT(row_num) FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY created_date DESC) AS row_num
                            FROM `my-dw-project-01.bq_upload.stg_bq_project`
                            WHERE created_date = CURRENT_DATE
                        )
                    WHERE row_num = 1) s
                    ON f.product_key = s.product_key
                    WHEN MATCHED THEN
                      UPDATE SET f.actual_price = s.actual_price, f.discounted_price = s.discounted_price,
                      f.discount_percentage = s.discount_percentage
                    WHEN NOT MATCHED THEN
                      INSERT (actual_price, discounted_price, discount_percentage, product_key)
                      VALUES (s.actual_price, s.discounted_price, s.discount_percentage, s.product_key)
                    """

        insert(insert_query, table_name, table_name_bq, column_name)
        return None

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


extract_transform()

load_dim_review()

load_dim_user()

load_dim_product()

load_surrogate_keys()

transform_load_fact_table()