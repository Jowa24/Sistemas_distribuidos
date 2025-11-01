# Use a slim, modern Python image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
# This step is cached by Docker and only re-runs if requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your Python scripts into the container
COPY *.py .

# Default command (this will be overridden by docker-compose.yml)
CMD ["bash"]

