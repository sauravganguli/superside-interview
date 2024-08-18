import requests
import pandas as pd
import os
import sys
import time
import logging
from tabulate import tabulate


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_api(url, headers, params, max_retries=3):
    """
    Fetch data from the Crunchbase API with retry mechanism.

    Args:
    - url (str): The API endpoint URL.
    - headers (dict): The headers to be sent with the request.
    - params (dict): The query parameters for the API request.
    - max_retries (int): Maximum number of retries in case of failure.

    Returns:
    - dict: Parsed JSON response from the API.
    - None: If there's an error in fetching the data.
    """
    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raises HTTPError for bad responses
            logging.info("API call successful!")
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        
        attempts += 1
        if attempts < max_retries:
            logging.info(f"Retrying... ({attempts}/{max_retries})")
            time.sleep(2 ** attempts)  # Exponential backoff

    logging.error("Max retries reached. Failed to fetch data from the API.")
    return None


def extract_fields(data):
    """
    Extract specific fields from the JSON data.

    Args:
    - data (dict): The JSON data.

    Returns:
    - dict: A dictionary with the extracted fields.
    - None: If the extraction fails.
    """
    try:
        fields = data.get('cards', {}).get('fields', {})
        
        permalink = fields.get('identifier', {}).get('permalink')
        website_url = fields.get('website_url')
        updated_at = fields.get('updated_at')
        linkedin_value = fields.get('linkedin', {}).get('value')

        # Extract city, region, and country from location_identifiers nested object
        location_identifiers = fields.get('location_identifiers', [])

        city = next((item['value'] for item in location_identifiers if item['location_type'] == 'city'), None)
        region = next((item['value'] for item in location_identifiers if item['location_type'] == 'region'), None)
        country = next((item['value'] for item in location_identifiers if item['location_type'] == 'country'), None)

        extracted_data = {
            "permalink": permalink,
            "website_url": website_url,
            "updated_at": updated_at,
            "linkedin": linkedin_value,
            "city": city,
            "region": region,
            "country": country
        }
        return extracted_data
    
    except KeyError as key_err:
        logging.error(f"Key error: {key_err}. The API structure may have changed.")
        return None
    except TypeError as type_err:
        logging.error(f"Type error: {type_err}. There might be an issue with the data structure.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during extraction: {e}")
        return None

    
def save_to_parquet(data, filename='company_data.parquet'):
    """
    Save the extracted data to a Parquet file.

    Args:
    - data (dict): The extracted data.
    - filename (str): The filename for the Parquet file.
    """
    try:
        df = pd.DataFrame([data])
        print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
        df.to_parquet(filename, engine='pyarrow', index=False)
        logging.info(f"Data has been saved to {filename}")
    except Exception as e:
        logging.error(f"An error occurred while saving to Parquet: {e}")

        
def main():
    # Get the API key from a env variable
    try:
        # Read API key from environment variable
        api_key = os.getenv('CRUNCHBASE_API_KEY')

        if not api_key:
            logging.error("API key not found. Please set the CRUNCHBASE_API_KEY environment variable.")
            sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred while reading the API key: {e}")
        sys.exit(1)

    # Construct the URL with the provided permalink
    url = f"https://api.crunchbase.com/v4/data/entities/organizations/konsus"
    headers = {
        "accept": "application/json",
        "X-cb-user-key": api_key
    }
    params = {
        "card_ids": ["fields"]
    }

    # Step 1: Fetch data from the API
    data = fetch_data_from_api(url, headers, params)

    # Step 2: Extract the required fields
    if data:
        extracted_data = extract_fields(data)
        
        # Step 3: Save the data to a Parquet file
        if extracted_data:
            save_to_parquet(extracted_data)
        else:
            logging.error("Data extraction failed. The required fields may not be present in the response.")
    else:
        logging.error("Failed to retrieve data from the API.")

        
if __name__ == "__main__":
    main()