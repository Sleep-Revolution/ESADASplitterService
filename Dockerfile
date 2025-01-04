# FROM python:3.10.6

# # Set the working directory inside the container
# WORKDIR /app

# # Copy the entire project directory to the container
# COPY . /app

# # Install dependencies
# RUN pip install -r requirements.txt

# # Expose any necessary ports (if applicable)

# # Set the entry point command to run your pipeline
# # CMD ["ls", "/app"]
# # CMD ["python3", "processor.py"]
# ENTRYPOINT ["python3", "processor.py"]


# SLIM VERSION

# Use a slim Python base image to reduce size
FROM python:3.10.6-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only the necessary files first (leverages Docker's caching)
COPY requirements.txt /app/

# Install dependencies with optimization
RUN pip install --no-cache-dir -r requirements.txt && \
    apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the rest of the application code
COPY . /app/

# Expose necessary ports if required (e.g., for web applications)
# EXPOSE 8080

# Set the entry point to run the application
ENTRYPOINT ["python3", "processor.py"]
