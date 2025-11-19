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
    # This converts the json data into python objects of lists and dictionaries (parsed JSON data).
    data = response.json()

    # Step 2: Upload data to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Since the data is in the form of a JSON array, convert the data to a JSON string and upload.
    # Without doing this, if you tried to upload this raw Python object you'd get a TypeError
    # because GCS expects a string or bytes — not a dict or list.
    # This is accomplished with json.dumps() used below.

    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type="application/json"
    )

    print(f'Data uploaded to GCS bucket:{bucket_name} and named as {destination_blob_name}')


upload_from_api(prd_url, 'my-dw-bucket-02', 'bq_source_data_01.json')

upload_from_api(sale_url, 'my-dw-bucket-02', 'bq_source_data_02.json')

upload_from_api(user_url, 'my-dw-bucket-02', 'bq_source_data_03.json')