# --------- Headers -------------
import time

from google.cloud import storage
import requests
import json


def get_data(key,locations):
    all_properties = {}
    for location in locations:
        data = []
        url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
        querystring = {"location": location}
        headers = {
            "X-RapidAPI-Key": key,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)

        pages = response.json()['totalPages']
        current_page = response.json()['currentPage'] + 1
        data.append(json.dumps(response.json()))
        while current_page <= pages:
            time.sleep(1)
            url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
            querystring = {"location": location ,'page': current_page}
            headers = {
                "X-RapidAPI-Key": key,
                "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
            }
            response = requests.get(url, headers=headers, params=querystring)
            current_page += 1
            data.append(json.dumps(response.json()))

        all_properties[location] = data
    return all_properties


def upload_to_gcp(bucket_name, properties,location):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    trg_file = 'raw_data'
    print('Uploading Data to DataLake')
    for property in properties:
        count = 0
        city = property.split(",")[0].replace(" ", "_")
        for prop in properties[property]:
            count += 1
            blob = bucket.blob(f'{trg_file}/{city}_{count}')
            blob.upload_from_string(prop, content_type='application/json')
    print("Upload Successful!")


if __name__ == "__main__":
    api_key = "bf61c16b70msh7a52ed39ef8980bp179c6bjsnca5314259972"
    property_location = ["santa monica, ca","syracuse, ny"]
    gcp_bucket = 'real-estate-datalake'
    property_data = get_data(api_key, property_location)
    upload_to_gcp(gcp_bucket, property_data, property_location)