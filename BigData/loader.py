import pandas as pd
from google.cloud import storage

# conversion to .csv if needed
# df = pd.read_excel('bigdata/bq_source_data_03.xlsx')
# df.to_csv('bigdata/bigdata03.csv', index=False)


# Defining the function that uploads a file to the bucket.
def upload_blob(source_file_name, bucket_name, destination_blob_name):

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    df = pd.read_csv(source_file_name)
    # turns invalid values into NaN, was necessary to address an error due to loading
    # df['rating'] = pd.to_numeric(df['rating'], errors='coerce')

    with blob.open('w', content_type='text/csv') as destination_file:
        df.to_csv(destination_file, index=True)

    print(f'Data uploaded to gcs bucket:{bucket_name} and named as {destination_blob_name}')


upload_blob('bigdata/bigdata03.csv', 'my-dw-bucket-01', 'bq_source_data_03')
