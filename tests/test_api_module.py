import pytest
from unittest.mock import patch
from api_module import fetch_data_from_api, extract_fields, save_to_parquet
import pandas as pd


@patch('api_module.requests.get')
def test_fetch_data_from_api(mock_get):
    url = "https://api.crunchbase.com/v4/data/entities/organizations/test"
    headers = {"accept": "application/json", "X-cb-user-key": "test_api_key"}
    params = {"card_ids": ["fields"]}

    # Mock a successful API response
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"cards": {"fields": {}}}

    data = fetch_data_from_api(url, headers, params)
    assert data == {"cards": {"fields": {}}}

    # Mock a failed API response
    mock_get.return_value.status_code = 404
    data = None
    assert data is None


def test_extract_fields():
    # Test extraction with valid data
    sample_data = {
        "cards": {
            "fields": {
                "identifier": {"permalink": "test-permalink"},
                "website_url": "https://test.com",
                "updated_at": "2024-02-01T05:45:15Z",
                "linkedin": {"value": "https://linkedin.com/test"},
                "location_identifiers": [
                    {"location_type": "city", "value": "Test City"},
                    {"location_type": "region", "value": "Test Region"},
                    {"location_type": "country", "value": "Test Country"}
                ]
            }
        }
    }
    extracted_data = extract_fields(sample_data)
    assert extracted_data == {
        "permalink": "test-permalink",
        "website_url": "https://test.com",
        "updated_at": "2024-02-01T05:45:15Z",
        "linkedin": "https://linkedin.com/test",
        "city": "Test City",
        "region": "Test Region",
        "country": "Test Country"
    }

    # Test extraction with missing fields
    sample_data = {"cards": {"fields": {}}}
    extracted_data = extract_fields(sample_data)
    assert extracted_data == {
        "permalink": None,
        "website_url": None,
        "updated_at": None,
        "linkedin": None,
        "city": None,
        "region": None,
        "country": None
    }


def test_save_to_parquet(tmp_path):
    # Create sample data
    sample_data = {
        "permalink": "test-permalink",
        "website_url": "https://test.com",
        "updated_at": "2024-02-01T05:45:15Z",
        "linkedin": "https://linkedin.com/test",
        "city": "Test City",
        "region": "Test Region",
        "country": "Test Country"
    }

    # Save to a temporary file
    save_to_parquet(sample_data, tmp_path / "test.parquet")

    # Load the file and check contents
    df = pd.read_parquet(tmp_path / "test.parquet")
    assert df.iloc[0].to_dict() == sample_data
