import requests
from google.cloud import storage
import json

prd_url = 'https://fakestoreapi.com/products'
sale_url = 'https://fakestoreapi.com/carts'
user_url = 'https://fakestoreapi.com/users'


def upload_from_api(url, bucket_name, destination_blob_name):
    # Step 1: Fetch data from API
    response = requests.get(url)
    response.raise_for_status()  # Raises HTTPError for bad responses
    data = response.json()  # Parsed JSON data

    # Step 2: Upload data to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Using json.dumps() will convert the Python object (list of dictionaries) into a json formatted string
    # however there are cases where this is not ideal. For example, BigQuery has a limitation on how the json array
    # is structured. It expects one JSON object (dictionary) per line and also not wrapped in [...]
    # (like a list would be). This json form is called newline delimited json (NDJSON) created below:

    blob.upload_from_string(
        data='\n'.join(json.dumps(record) for record in data),
        content_type="application/json"
    )

    print(f'NDJSON uploaded to gcs bucket:{bucket_name} and named as {destination_blob_name}')


upload_from_api(prd_url, 'my-dw-bucket-02', 'bq_source_data_04.json')

upload_from_api(sale_url, 'my-dw-bucket-02', 'bq_source_data_05.json')

upload_from_api(user_url, 'my-dw-bucket-02', 'bq_source_data_06.json')