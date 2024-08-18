# Crunchbase API Integration

This project contains a Python script to interact with the Crunchbase API, extract data, and save it in Parquet format. Additionally, it includes test coverage using `pytest`.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Building the Docker Image](#building-the-docker-image)
- [Running the Docker Container](#running-the-docker-container)
- [Running Tests](#running-tests)

## Prerequisites

Ensure you have the following installed on your machine:
- [Git](https://git-scm.com/)
- [Docker](https://www.docker.com/get-started)

## Getting Started

### Clone the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/sauravganguli/superside-interview.git
cd superside-interview
```
### Understanding the Repository structure
* `crunchbase_api.py` - The main Python script that interacts with the Crunchbase API, extracts data, processes it, and saves it to a Parquet file.
* `requirements.txt` - A list of Python dependencies required to run the script.
* `Dockerfile` - A Dockerfile to build the Docker image.
* `tests/` - A directory containing test files.
* `tests/test_crunchbase_api.py` - A test file containing test cases for the main Python script.
* `/docs` - A directory containing the documentation for the assignments.
* `/docs/assignment.md` - The assignment document with detailed explanation for each task.
* `/docs/architecture_diagram.png` - The data architecture diagram for Task 1.
* `/docs/dwh_concept.png` - The data warehouse concept diagram for Task 2.

## Building the Docker Image
To build the Docker image, use the following command:
    
```bash
docker build -t crunchbase_api:latest .
```

This command will:
* Set up the Python environment within the Docker container.
* Install the necessary Python dependencies.
* Copy your code into the container.

## Running the Docker Container
To run the Docker container, use the following command:

```bash
docker run -e CRUNCHBASE_API_KEY=your_api_key_here crunchbase_api:latest
```

This command will:

Run the main Python script (`crunchbase_api.py`) which fetches data from the Crunchbase API, processes it, prints inline to view the data extracted and saves it to a Parquet file.
Run the test suite using pytest to ensure the code works as expected.

### Example Output
After running the container, you should see logs indicating:
* API calls made.
* Data extraction and processing.
* Superside data retrieved from Crunchbase.
* Test results from pytest.

## Running Tests
Tests are automatically executed when the Docker container is run. However, if you wish to run tests manually inside the container, you can do so by executing:

```bash
docker run --entrypoint pytest crunchbase_api:latest --maxfail=1 --disable-warnings
```

This command will run the test suite using pytest with options to stop after the first failure and disable warnings.