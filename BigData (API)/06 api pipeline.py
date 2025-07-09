import pandas as pd
from google.cloud import bigquery
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
from time import time

client = bigquery.Client()


# Loading raw data blobs from GCS bucket to BigQuery staging
def extract_product():
    try:
        destination_table = 'bigdata_api.stg_prod_raw'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        uri = 'gs://my-dw-bucket-02/bq_source_data_04.json'

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Loading completed for product data.')

    except Exception as error:
        print(error)


def extract_sales():
    try:
        destination_table = 'bigdata_api.stg_sales_raw'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        uri = 'gs://my-dw-bucket-02/bq_source_data_05.json'

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Loading completed for sales data.')

    except Exception as error:
        print(error)


def extract_user():
    try:
        destination_table = 'bigdata_api.stg_user_raw'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True
        )

        uri = 'gs://my-dw-bucket-02/bq_source_data_06.json'

        load_job = client.load_table_from_uri(uri, destination_table, job_config=job_config)
        load_job.result()

        print(f'Loading completed for user data.')

    except Exception as error:
        print(error)


# Transformation of raw product data into its cleaned form
def transform_product():
    try:
        dp = read_gbq('bigdata_api.stg_prod_raw', 'my-dw-project-01')
        raw_prod = dp.copy()

        clean_prod = raw_prod.join(pd.json_normalize(raw_prod['rating']))
        clean_prod = clean_prod.drop(columns=['rating'])
        to_gbq(clean_prod, 'bigdata_api.stg_prod_clean', project_id='my-dw-project-01', if_exists='fail')

        return print('Product data transformed successfully.')

    except Exception as error:
        print(error)


# Transformation of raw sales data into its cleaned form
def transform_sales():
    try:
        ds = read_gbq('bigdata_api.stg_sales_raw', 'my-dw-project-01')
        raw_sales = ds.copy()

        raw_sales_explode = raw_sales.explode('products')
        products_expanded = raw_sales_explode['products'].apply(pd.Series)
        clean_sales = pd.concat([raw_sales_explode, products_expanded], axis=1)
        # Concat is used here as an alternative to join().
        clean_sales = clean_sales.drop(columns=['products'])
        clean_sales['date'] = pd.to_datetime(clean_sales['date']).dt.date
        clean_sales['month'] = pd.to_datetime(clean_sales['date']).dt.month
        clean_sales['year'] = pd.to_datetime(clean_sales['date']).dt.year
        to_gbq(clean_sales, 'bigdata_api.stg_sales_clean', project_id='my-dw-project-01',
               if_exists='fail')

        return print('Sales data transformed successfully.')

    except Exception as error:
        print(error)


# Transformation of raw user data into its cleaned form
def transform_user():
    try:
        du = read_gbq('bigdata_api.stg_user_raw', 'my-dw-project-01')
        raw_user = du.copy()

        raw_user = raw_user.join(pd.json_normalize(raw_user['address']))
        raw_user = raw_user.rename(columns={'geolocation.lat': 'geolocation_lat'})
        raw_user = raw_user.rename(columns={'geolocation.long': 'geolocation_long'})
        raw_user = raw_user.join(pd.json_normalize(raw_user['name']))
        raw_user = raw_user.drop(columns=['address'])
        clean_user = raw_user.drop(columns=['name'])
        clean_user['firstname'] = clean_user['firstname'].str.capitalize()
        clean_user['lastname'] = clean_user['lastname'].str.capitalize()
        to_gbq(clean_user, 'bigdata_api.stg_user_clean', project_id='my-dw-project-01', if_exists='fail')

        return print('User data transformed successfully.')

    except Exception as error:
        print(error)


# Creating a combined staging table from all 3 cleaned data sources.
def create_combo_staging():
    try:
        create_combined_stg_table = '''
        CREATE TABLE bigdata_api.stg_combo_clean_table AS
        SELECT a.id, `userId`, date, month, year, a.__v, `productId`, quantity, title, price, 
        description, category, image, rate, count, email, username, 
        password, phone, city, street, number, zipcode, geolocation_lat, 
        geolocation_long, firstname, lastname 
        FROM bigdata_api.stg_sales_clean AS a
        JOIN bigdata_api.stg_prod_clean AS b on a.`productId` = b.id
        JOIN bigdata_api.stg_user_clean AS c on a.`userId` = c.id
        '''

        query_job = client.query(create_combined_stg_table)
        query_job.result()

        print('Clean staging table created successfully.')

        # Creation of additional columns on staging to hold surrogate keys
        # to be filled later with dim table data.
        try:
            update_staging = '''
            ALTER TABLE bigdata_api.stg_combo_clean_table
            ADD COLUMN product_key STRING,
            ADD COLUMN user_key STRING,
            ADD COLUMN date_key STRING
            '''

            query_job = client.query(update_staging)
            query_job.result()

            print('Surrogate key columns added to staging.')

        except Exception as error:
            print(f'Issue with creating surrogate key columns: {error}')

    except Exception as error:
        print(f'Issue with combo staging table creation: {error}')


# Creating target tables.
def create_tables():
    try:
        create_dim_product = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_product (
        product_key STRING DEFAULT GENERATE_UUID(),
        product_id INT64,
        product_name STRING,
        description STRING,
        category STRING,
        image STRING,
        rating FLOAT64
        )'''

        query_job = client.query(create_dim_product)
        query_job.result()

        create_dim_city = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_city (
        city_key STRING DEFAULT GENERATE_UUID(),
        city STRING
        )'''

        query_job = client.query(create_dim_city)
        query_job.result()

        create_dim_user = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_user (
        user_key STRING DEFAULT GENERATE_UUID(),
        user_id INT,
        first_name STRING,
        last_name STRING,
        email STRING,
        username STRING,
        password STRING,
        phone STRING,
        street STRING,
        number INT64,
        zipcode STRING,
        latitude FLOAT64,
        longitude FLOAT64,
        city_key STRING
        )'''

        query_job = client.query(create_dim_user)
        query_job.result()

        create_dim_date = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.dim_date (
        date_key STRING DEFAULT GENERATE_UUID(),
        sale_date DATE,
        month INT64,
        year INT64
        )'''

        query_job = client.query(create_dim_date)
        query_job.result()

        create_fact_table = '''
        CREATE TABLE IF NOT EXISTS bigdata_api.fact_sale_table (
        sale_key STRING DEFAULT GENERATE_UUID(),
        sale_id INT64,
        product_key STRING,
        user_key STRING,
        date_key STRING,
        price FLOAT64,
        quantity INT64,
        total_sale FLOAT64,
        stock INT64
        )'''

        query_job = client.query(create_fact_table)
        query_job.result()

        print('All target tables created successfully.')

    except Exception as error:
        print(f'Issue with table creation: {error}')


# Loading the target tables.
def load_dim_product():
    table_name = 'dim_product'

    try:
        dp = read_gbq('bigdata_api.stg_combo_clean_table', 'my-dw-project-01')
        product = dp[['productId', 'title', 'description', 'category', 'image', 'rate']].copy()
        product = product.rename(columns={'productId': 'product_id', 'title': 'product_name', 'rate': 'rating'})
        product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')

        t1 = time()
        to_gbq(product, 'bigdata_api.dim_product', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2-t1

        print(f'Rows 0 to {len(product)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Issue with loading {table_name}: {error}')


def load_dim_user():
    table_name = 'dim_user'

    try:
        du = read_gbq('bigdata_api.stg_combo_clean_table', 'my-dw-project-01')
        user = du[['userId', 'email', 'username', 'password', 'phone', 'firstname', 'lastname',
                   'street', 'number', 'zipcode', 'geolocation_lat', 'geolocation_long']].copy()
        user = user.rename(
            columns={'userId': 'user_id', 'firstname': 'first_name', 'lastname': 'last_name',
                     'geolocation_lat': 'latitude', 'geolocation_long': 'longitude'}
        )
        user['password'] = '***Masked***'
        user['phone'] = '***Masked***'
        user['street'] = user['street'].str.title()
        user = user.drop_duplicates(subset=['user_id', 'first_name', 'last_name'], keep='first')

        t1 = time()
        to_gbq(user, 'bigdata_api.dim_user', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(user)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Issue with loading {table_name}: {error}')


def load_dim_date():
    table_name = 'dim_date'

    try:
        dt = read_gbq('bigdata_api.stg_combo_clean_table', 'my-dw-project-01')
        date = dt[['date', 'month', 'year']].copy()
        date = date.rename(columns={'date': 'sale_date'})
        date = date.drop_duplicates(subset=['sale_date'], keep='first')

        t1 = time()
        to_gbq(date, 'bigdata_api.dim_date', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(date)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Issue with loading {table_name}: {error}')


# Filling surrogate key columns with the actual surrogate keys.
def upload_surrogate_keys():
    try:
        product_key = '''
        UPDATE bigdata_api.stg_combo_clean_table AS s SET product_key = p.product_key 
        FROM bigdata_api.dim_product AS p WHERE s.`productId` = p.product_id AND
        s.title = p.product_name'''
        query_job = client.query(product_key)
        query_job.result()

        user_key = '''
        UPDATE bigdata_api.stg_combo_clean_table AS s SET user_key = u.user_key 
        FROM bigdata_api.dim_user AS u WHERE s.`userId` = u.user_id AND
        s.firstname = u.first_name AND s.lastname = u.last_name'''
        query_job = client.query(user_key)
        query_job.result()

        date_key = '''
        UPDATE bigdata_api.stg_combo_clean_table AS s SET date_key = d.date_key 
        FROM bigdata_api.dim_date AS d WHERE s.date = d.sale_date'''
        query_job = client.query(date_key)
        query_job.result()

        # Fetching surrogate keys from dim_city and loading to dim_user.

        # Note that since there is only one attribute and no transformation required
        # it is more efficient loading dim_city this way than through a dataframe.
        # This also explains why its loading is designed to happen at this point i.e. together with
        # similar sql commands.

        load_dim_city = '''
        INSERT INTO bigdata_api.dim_city (city)
        SELECT DISTINCT city FROM bigdata_api.stg_combo_clean_table'''
        query_job = client.query(load_dim_city)
        query_job.result()

        # Update in BigQuery expects unique records between the source and target rows
        # Therefore it is necessary to filter staging (source) for unique records before doing the
        # comparison (WHERE u.user_id = j.`userId`)
        update_dim_user = '''
        UPDATE bigdata_api.dim_user AS u SET city_key = j.city_key
        FROM (
            SELECT * EXCEPT(row_num) FROM(
                SELECT *, ROW_NUMBER() OVER(PARTITION BY `userId`) AS row_num
                FROM bigdata_api.stg_combo_clean_table AS s
                JOIN bigdata_api.dim_city AS c ON s.city = c.city
                ) WHERE row_num = 1
            ) AS j
        WHERE u.user_id = j.`userId`
        '''
        query_job = client.query(update_dim_user)
        query_job.result()

    except Exception as error:
        print(f'Issue with surrogate keys loading step: {error}')


def load_fact_sale():
    table_name = 'fact_sale_table'

    try:
        df = read_gbq('bigdata_api.stg_combo_clean_table', 'my-dw-project-01')
        fact = df[['id', 'product_key', 'user_key', 'date_key', 'price', 'quantity', 'count']].copy()
        fact = fact.rename(columns={'id': 'sale_id', 'count': 'stock'})
        fact['total_sale'] = fact['price'] * fact['quantity']

        t1 = time()
        to_gbq(fact, 'bigdata_api.fact_sale_table', project_id='my-dw-project-01', if_exists='append')
        t2 = time()

        load_time = t2 - t1

        print(f'Rows 0 to {len(fact)} loaded successfully for {table_name} in {load_time}s')

    except Exception as error:
        print(f'Issue with loading {table_name}: {error}')


extract_product()
extract_sales()
extract_user()

transform_product()
transform_sales()
transform_user()

create_combo_staging()
create_tables()

load_dim_product()
load_dim_user()
load_dim_date()
upload_surrogate_keys()
load_fact_sale()