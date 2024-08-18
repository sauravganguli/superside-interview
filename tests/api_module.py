import requests
import pandas as pd

def fetch_data_from_api(url, headers, params):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def extract_fields(data):
    try:
        fields = data.get('cards', {}).get('fields', {})
        return {
            "permalink": fields.get('identifier', {}).get('permalink'),
            "website_url": fields.get('website_url'),
            "updated_at": fields.get('updated_at'),
            "linkedin": fields.get('linkedin', {}).get('value'),
            "city": next((item['value'] for item in fields.get('location_identifiers', []) if item['location_type'] == 'city'), None),
            "region": next((item['value'] for item in fields.get('location_identifiers', []) if item['location_type'] == 'region'), None),
            "country": next((item['value'] for item in fields.get('location_identifiers', []) if item['location_type'] == 'country'), None),
        }
    except (KeyError, TypeError) as e:
        print(f"Error extracting fields: {e}")
        return None

def save_to_parquet(data, filename='company_data.parquet'):
    df = pd.DataFrame([data])
    df.to_parquet(filename, engine='pyarrow', index=False)
