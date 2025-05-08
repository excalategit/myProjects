#!/usr/bin/env python
# coding: utf-8

# The design here (based on best practice) is to load data to a staging table, transform
# and export dimension table data to their tables, create surrogate keys on those tables and
# import the surrogate keys back to the staging table.
# Afterward transform and export fact table data together with all surrogate keys to the
# fact table.
# Note that this design sometimes require some transformation to be done on the data in staging
# before exporting to dimension tables. This is because at the step where surrogate keys are
# loaded back to staging, the action is based on a comparison between staging and dimension
# table data which will fail if one table is transformed and the other not.

import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Creating the fact and dimension tables where the transformed data will be loaded to.
# Here the connection is defined with an additional specification for the schema since here
# the tables are not created on the default public schema.

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
            options='-c search_path=amazon') as connection:
        # This is how to indicate the schema of interest

        with connection.cursor() as cursor:
            create_dim_product = '''
            CREATE TABLE IF NOT EXISTS dim_product (
            product_key SERIAL PRIMARY KEY,
            product_id TEXT,
            product_name TEXT,
            category TEXT,
            about_product TEXT,
            img_link TEXT,
            product_link TEXT,
            rating TEXT,
            rating_count TEXT
            )'''

            cursor.execute(create_dim_product)

            create_dim_user = '''
            CREATE TABLE IF NOT EXISTS dim_user (
            user_key SERIAL PRIMARY KEY,
            user_id TEXT,
            user_name TEXT
            )'''

            cursor.execute(create_dim_user)

            create_dim_review = '''
            CREATE TABLE IF NOT EXISTS dim_review (
            review_key SERIAL PRIMARY KEY,
            review_id TEXT,
            review_title TEXT,
            review_content TEXT
            )'''

            cursor.execute(create_dim_review)

            create_fact_sale = '''
            CREATE TABLE IF NOT EXISTS fact_sale (
            sale_key SERIAL PRIMARY KEY,
            product_key INT REFERENCES dim_product (product_key),
            user_key INT REFERENCES dim_user (user_key),
            review_key INT REFERENCES dim_review (review_key),
            "actual_price (PLN)" FLOAT,
            "discounted_price (PLN)" FLOAT,
            discount_percentage TEXT
            )'''

            cursor.execute(create_fact_sale)

except Exception as error:
    print(error)

finally:
    if connection is not None:
        connection.close()


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


# Defining the function that extracts and transforms to staging

def extract_transform():
    connection = None

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_excel('amazon.xlsx')
        source_table.to_sql('stg_amazon_sales_report', engine, index=False, if_exists='replace')

        # Addition of surrogate key columns to staging
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASS')

        with psycopg2.connect(
                host='localhost',
                dbname='Destination',
                user=db_user,
                password=db_password,
                port=5432) as connection:

            with connection.cursor() as cursor:
                staging_update = '''ALTER TABLE stg_amazon_sales_report
                ADD COLUMN product_key INT,
                ADD COLUMN user_key INT,
                ADD COLUMN review_key INT
                '''

                cursor.execute(staging_update)

                return print('Extraction to staging completed')

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


# Defining the function that loads the transformed data to the dimension tables

def load_dim_product():
    engine = create_engine('postgresql:///Destination')

    dp = pd.read_sql('stg_amazon_sales_report', engine)
    product = dp[['product_id', 'product_name', 'category', 'about_product', 'img_link', 'product_link',
                  'rating', 'rating_count']].copy()
    product['rating_count'] = product['rating_count'].fillna(1)
    product = product.drop_duplicates(subset=['product_id', 'product_name'], keep='first')
    # It is best practice to deduplicate dim tables using business keys alone.
    product = product.to_dict('records')

    insert_query = '''INSERT into amazon.dim_product (
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

    insert(insert_query, product)
    return print('dim_product loaded successfully')


def load_dim_user():
    engine = create_engine('postgresql:///Destination')

    du = pd.read_sql('stg_amazon_sales_report', engine)
    user = du[['user_id', 'user_name']].copy()
    user = user.drop_duplicates()
    user = user.to_dict('records')

    insert_query = '''INSERT into amazon.dim_user (
    user_id,
    user_name
    ) 
    VALUES (
    %(user_id)s,
    %(user_name)s
    )'''

    insert(insert_query, user)
    return print('dim_user loaded successfully')


def load_dim_review():
    engine = create_engine('postgresql:///Destination')

    drr = pd.read_sql('stg_amazon_sales_report', engine)
    review = drr[['review_id', 'review_title', 'review_content']].copy()
    review = review.drop_duplicates(subset=['review_id', 'review_title'], keep='first')
    review = review.to_dict('records')

    insert_query = '''INSERT into amazon.dim_review (
    review_id,
    review_title,
    review_content
    )
    VALUES (
    %(review_id)s,
    %(review_title)s,
    %(review_content)s
    )'''

    insert(insert_query, review)
    return print('dim_review loaded successfully')


# Defining the function that loads the surrogate keys to staging

def load_surr_keys():
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
                product_key = '''UPDATE stg_amazon_sales_report AS s SET product_key = p.product_key 
                FROM amazon.dim_product AS p WHERE s.product_id = p.product_id AND
                s.product_name = p.product_name'''
                cursor.execute(product_key)

                user_key = '''UPDATE stg_amazon_sales_report AS s SET user_key = u.user_key 
                FROM amazon.dim_user AS u WHERE s.user_id = u.user_id AND
                s.user_name = u.user_name'''
                cursor.execute(user_key)

                review_key = '''UPDATE stg_amazon_sales_report AS s SET review_key = rr.review_key 
                FROM amazon.dim_review AS rr WHERE s.review_id = rr.review_id AND
                s.review_title = rr.review_title'''
                cursor.execute(review_key)

                return print('staging updated with surr. keys successfully')

    except Exception as error:
        print(error)

    finally:
        if connection is not None:
            connection.close()


# Finally, defining the function that transforms and loads the fact table together with the surrogate keys

def transform_load_fact_table():
    engine = create_engine('postgresql:///Destination')

    dg = pd.read_sql('stg_amazon_sales_report', engine)
    fact = dg[['discounted_price', 'actual_price', 'discount_percentage',
               'product_key', 'user_key', 'review_key']].copy()

    fact['discounted_price'] = fact['discounted_price'].str.replace('₹', '')
    fact['discounted_price'] = fact['discounted_price'].str.replace(',', '').astype(float)
    fact['actual_price'] = fact['actual_price'].str.replace('₹', '')
    fact['actual_price'] = fact['actual_price'].str.replace(',', '').astype(float)
    fact = fact.to_dict('records')

    insert_query = '''INSERT into amazon.fact_sale (
    product_key,
    user_key,
    review_key, 
    "actual_price (PLN)",
    "discounted_price (PLN)",
    discount_percentage
    ) 
    VALUES (
    %(product_key)s,
    %(user_key)s,
    %(review_key)s,
    %(actual_price)s,
    %(discounted_price)s,
    %(discount_percentage)s
    )'''

    insert(insert_query, fact)
    return print('fact_table loaded successfully')


extract_transform()

load_dim_product()

load_dim_user()

load_dim_review()

load_surr_keys()

transform_load_fact_table()


