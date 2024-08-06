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
# Set environment variable for Streamlit to run on 0.0.0.0
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Use a shell to run the Streamlit app and make it accessible from outside the container
CMD ["streamlit", "run", "--server.address", "0.0.0.0", "--server.port", "8501", "app.py"]
