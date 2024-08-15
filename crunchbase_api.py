import requests
import pandas as pd
import sys

def fetch_data_from_api(url, headers, params):
    """
    Fetch data from the Crunchbase API.

    Args:
    - url (str): The API endpoint URL.
    - headers (dict): The headers to be sent with the request.
    - params (dict): The query parameters for the API request.

    Returns:
    - dict: Parsed JSON response from the API.
    - None: If there's an error in fetching the data.
    """
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            # Success, return the data
            print("API call successful!")
            data = response.json()
            return(data)
        else:
            # Error handling
            print(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
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
        # Extract the necessary fields from 'cards' > 'fields'
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
        print(f"Key error: {key_err}. The API structure may have changed.")
        return None
    except TypeError as type_err:
        print(f"Type error: {type_err}. There might be an issue with the data structure.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
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
        df.to_parquet(filename, engine='pyarrow', index=False)
        print(f"Data has been saved to {filename}")
    except Exception as e:
        print(f"An error occurred while saving to Parquet: {e}")

        
def main():
    # Get the API key from a secrets file
    try:
        with open('secrets.txt', 'r') as file:
            api_key = file.read().strip()
    except Exception as e:
        print(f"An error occurred while reading the secrets file: {e}")
        sys.exit(1)
        
    # Get the permalink as a user input
    permalink = input("Enter the permalink of the company to search: ").strip()

    # Construct the URL with the provided permalink
    url = f"https://api.crunchbase.com/v4/data/entities/organizations/{permalink}"
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
            print("Data extraction failed. The required fields may not be present in the response.")
    else:
        print("Failed to retrieve data from the API.")

        
if __name__ == "__main__":
    main()