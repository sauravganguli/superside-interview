# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the environment variable to ensure Python outputs are sent straight to the terminal (unbuffered)
ENV PYTHONUNBUFFERED=1

# Run the Python script followed by pytest
CMD ["sh", "entrypoint.sh"]