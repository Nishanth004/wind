# Dockerfile
FROM python:3.9-slim

WORKDIR /app

# Copy only necessary files for build context
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files (adjust if needed, but simple for now)
COPY . .

# Default command (will be overridden by docker-compose)
CMD ["python", "--version"]