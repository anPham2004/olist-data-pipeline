# Base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all code into the container
COPY . .

# Copy entrypoint file to the root directory to avoid being overwritten by Volume Mount
COPY entrypoint.sh /entrypoint.sh

# Grant execute permission for the entrypoint script
RUN chmod +x /entrypoint.sh

# Initialize port for Streamlit
EXPOSE 8501

# Set the entrypoint to be run when the container starts
ENTRYPOINT ["/entrypoint.sh"]