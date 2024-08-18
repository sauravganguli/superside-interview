#!/bin/sh

# Run the Python script
python crunchbase_api.py

# Run pytest for testing
pytest --maxfail=1 --disable-warnings