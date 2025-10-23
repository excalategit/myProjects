# Script to quickly load sample dataset to Postgres as source data.

import pandas as pd
from sqlalchemy import create_engine


def extract_transform():

    try:
        engine = create_engine('postgresql:///Destination')

        source_table = pd.read_excel('bigquery/bigquery.xlsx')

        source_table.to_sql('bq_source_data', engine, index=False, if_exists='replace')

        print('Loading completed successfully.')

    except Exception as e:
        print ('Issue with loading.', e)


extract_transform()