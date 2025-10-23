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
    # This converts the json data into python objects of lists and dictionaries.

    # Step 2: Upload data to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert the data to a JSON string and upload. This is accomplished with json.dumps() used below.
    # Without doing this, the data would be in the form of a json array, if you tried to upload this raw Python object,
    # you'd get a TypeError, because GCS expects a string or bytes — not a dict or list.

    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type="application/json"
    )

    print(f'Data uploaded to gcs bucket:{bucket_name} and named as {destination_blob_name}')


upload_from_api(prd_url, 'my-dw-bucket-02', 'bq_source_data_04.json')

upload_from_api(sale_url, 'my-dw-bucket-02', 'bq_source_data_05.json')

upload_from_api(user_url, 'my-dw-bucket-02', 'bq_source_data_06.json')