# Use the official Python 3.10 image as the base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY pipelines/analytics_platform/requirements.txt ./requirements.txt

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Streamlit app into the container
COPY pipelines/analytics_platform/app.py ./app.py

# Expose the port Streamlit runs on
EXPOSE 8501

# Set the command to run the Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
