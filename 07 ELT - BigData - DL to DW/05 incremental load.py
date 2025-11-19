import pandas as pd
from google.cloud import bigquery
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from datetime import datetime
from datetime import timedelta
from time import time

client = bigquery.Client()

# Fetching a raw data blob from GCS bucket and loading to BigQuery staging
# This requires a different syntax from the usual read_sql, read_csv etc.
uri = 'gs://my-dw-bucket-01/bq_source_data_03'
destination_table = 'bigdata_load.stg_bq_raw'

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
)

before_count = client.get_table(destination_table).num_rows

load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
load_job.result()

after_count = client.get_table(destination_table).num_rows
inserted_count = after_count - before_count

print(f'Loaded {inserted_count} rows')


# From staging, the raw data is extracted and transformed to a second staging table containing cleaned data
def extract_transform():
    try:
        ds = read_gbq('bigdata_load.bq_raw_staging', 'my-dw-project-01')
        source_table = ds.copy()

        # The condition here fetches data with a modified date of 'yesterday' for incremental loading.
        source_table['modified_date'] = pd.to_datetime(source_table['modified_date']).dt.date
        source_table = source_table[source_table['modified_date'] == datetime.today().date() - timedelta(days=1)]

        source_table['user_id'] = source_table['user_id'].astype(str)
        source_table['user_id'] = source_table['user_id'].str.split(',')
        source_table['user_name'] = source_table['user_name'].astype(str)
        source_table['user_name'] = source_table['user_name'].str.split(',')
        source_table['review_id'] = source_table['review_id'].astype(str)
        source_table['review_id'] = source_table['review_id'].str.split(',')
        source_table['review_title'] = source_table['review_title'].astype(str)
        source_table['review_title'] = source_table['review_title'].str.split(',')
        source_table = source_table.explode(['user_id', 'user_name', 'review_id', 'review_title'])

        source_table['rating_count'] = source_table['rating_count'].fillna(1)

        source_table['discounted_price'] = source_table['discounted_price'].astype(str)
        source_table['discounted_price'] = source_table['discounted_price'].str.replace('₹', '')
        source_table['discounted_price'] = source_table['discounted_price'].str.replace(',', '').astype(float)
        source_table['actual_price'] = source_table['actual_price'].astype(str)
        source_table['actual_price'] = source_table['actual_price'].str.replace('₹', '')
        source_table['actual_price'] = source_table['actual_price'].str.replace(',', '').astype(float)

        # source_table = source_table.rename(columns={'review_title': 'review_content'})
        # source_table = source_table.rename(columns={'discounted_price': 'discounted_price_pln'})
        # source_table = source_table.rename(columns={'actual_price': 'actual_price_pln'})

        source_table = source_table.drop_duplicates()

        # Finally, the data is assigned a created date of 'today' for audit purposes before loading to staging
        source_table['created_date'] = datetime.today().date()

        to_gbq(source_table, 'bigdata_load.stg_bq_clean',
               project_id='my-dw-project-01', if_exists='append')

        return print('Extraction to staging completed')

    except Exception as error:
        print(f'Extraction to staging failed: {error}')


def loader(insert_query, table_name, table_name_bq, column_name):
    try:
        t1 = time()
        query_job = client.query(insert_query)
        query_job.result()
        t2 = time()

        load_time = t2-t1

        print(f'Rows loaded successfully for {table_name} in {load_time}s')

        try:
            call_procedure = ''' 
                    call `bigdata_load.audit_table`(@table_name_bq, @column_name, @load_time)
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


def load_dim_product():
    table_name = 'dim_product'
    table_name_bq = 'bigdata_load.dim_product'
    column_name = 'product_id'

    try:
        insert_query = """
        MERGE `bigdata_load.dim_product` p
        USING (
            SELECT * FROM `bigdata_load.stg_bq_clean`
            WHERE created_date = CURRENT_DATE
            ) AS s
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

        loader(insert_query, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


def load_dim_user():
    table_name = 'dim_user'
    table_name_bq = 'bigdata_load.dim_user'
    column_name = 'user_id'

    try:
        insert_query = """
        MERGE `bigdata_load.dim_user` u
        USING (
            SELECT * FROM `bigdata_load.stg_bq_clean`
            WHERE created_date = CURRENT_DATE
            ) AS s
        ON u.user_id = s.user_id
        WHEN MATCHED THEN
          UPDATE SET u.user_name = s.user_name, u.last_updated_date = CURRENT_DATE
        WHEN NOT MATCHED THEN
          INSERT (user_id, user_name)
          VALUES (s.user_id, s.user_name)
        """

        loader(insert_query, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


def load_dim_review():
    table_name = 'dim_review'
    table_name_bq = 'bigdata_load.dim_review'
    column_name = 'review_id'

    try:
        insert_query = """
        MERGE `bigdata_load.dim_review` r
        USING (
            SELECT * FROM `bigdata_load.stg_bq_clean`
            WHERE created_date = CURRENT_DATE
            ) AS s
        ON r.review_id = s.review_id
        WHEN MATCHED THEN
          UPDATE SET r.review_title = s.review_title, r.last_updated_date = CURRENT_DATE
        WHEN NOT MATCHED THEN
          INSERT (review_id, review_title)
          VALUES (s.review_id, s.review_title)
        """

        loader(insert_query, table_name, table_name_bq, column_name)

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


def load_surrogate_keys():
    try:
        # Loading surrogate keys from dimension tables to staging.
        product_key = '''
        UPDATE bigdata_load.stg_bq_clean AS s SET product_key = p.product_key
        FROM bigdata_load.dim_product AS p WHERE s.created_date = CURRENT_DATE AND 
        s.product_id = p.product_id AND s.product_name = p.product_name
        '''
        query_job = client.query(product_key)
        query_job.result()

        user_key = '''
        UPDATE bigdata_load.stg_bq_clean AS s SET user_key = u.user_key
        FROM bigdata_load.dim_user AS u WHERE s.created_date = CURRENT_DATE AND 
        s.user_id = u.user_id AND s.user_name = u.user_name
        '''
        query_job = client.query(user_key)
        query_job.result()

        # Loading surrogate keys from staging to dim_review.

        # BigQuery, unlike Postgres requires the source table to have unique records of the
        # column that will be used for comparison with the target table i.e. the source_table cannot
        # contain multiple records of the same id while the target_table has only one with that same id,
        # which is the case here. The solution employed (Window Functions) creates a subset of
        # staging data containing unique review_ids with today's creation date, for the comparison.

        # Loading dim_product table's surrogate keys from staging to dim_review.
        load_prod_review = '''
        UPDATE bigdata_load.dim_review r SET product_key = s.product_key
        FROM (
            SELECT * EXCEPT(rank) FROM (
                SELECT *, RANK() OVER (PARTITION BY review_id ORDER BY created_date DESC) AS rank
                FROM bigdata_load.stg_bq_clean
                ) WHERE rank = 1
            ) AS s
        WHERE r.review_id = s.review_id
        '''
        query_job = client.query(load_prod_review)
        query_job.result()

        # Loading dim_user table's surrogate keys from staging to dim_review.
        load_user_review = '''
        UPDATE bigdata_load.dim_review AS r SET user_key = s.user_key
        FROM (
            SELECT * EXCEPT(rank) FROM (
                SELECT *, RANK() OVER (PARTITION BY review_id ORDER BY created_date DESC) AS rank
                FROM bigdata_load.stg_bq_clean
                ) WHERE rank = 1
            ) AS s
        WHERE r.review_id = s.review_id
        '''
        query_job = client.query(load_user_review)
        query_job.result()

        return print('All target tables updated with surrogate keys successfully')

    except Exception as error:
        print(f'Loading surrogate keys failed: {error}')


def transform_load_fact_table():
    table_name = 'fact_price'
    table_name_bq = 'bigdata_load.fact_price'
    column_name = 'product_key'

    try:
        insert_query = """
        INSERT INTO `bigdata_load.fact_price` 
        (actual_price, discounted_price, discount_percentage, product_key) (
            SELECT * EXCEPT(created_date, row_num) FROM (
                SELECT actual_price, discounted_price, CAST (discount_percentage as STRING) AS discount_percentage, 
                product_key, created_date, ROW_NUMBER() 
                OVER (PARTITION BY product_key ORDER BY created_date DESC) AS row_num
                FROM `bigdata_load.stg_bq_clean`
                WHERE created_date = CURRENT_DATE
                ) WHERE row_num = 1
            )
        """

        loader(insert_query, table_name, table_name_bq, column_name)
        return None

    except Exception as error:
        print(f'Potential issue with transformation step: {error}')


extract_transform()

load_dim_product()

load_dim_user()

load_dim_review()

load_surrogate_keys()

transform_load_fact_table()