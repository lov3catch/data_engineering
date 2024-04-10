# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set environment variables:
# - PYTHONDONTWRITEBYTECODE: Prevents Python from writing pyc files to disc (equivalent to python -B option)
# - PYTHONUNBUFFERED: Prevents Python from buffering stdout and stderr (equivalent to python -u option)
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory to /app
WORKDIR /app

# Install system dependencies
# This step is separated from the installation of Python dependencies to leverage Docker cache
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc6-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy only the requirements.txt file to leverage Docker cache
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code
COPY . /app

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variable for the Flask application to run in production mode
ENV FLASK_ENV=production

# Run the application
# Replace `yourapplication.py` with the file that runs your Flask app
#CMD ["gunicorn", "--bind", "0.0.0.0:8000", "main:app"]
