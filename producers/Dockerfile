# Base Python image
FROM python:3.11-slim

WORKDIR /app

# Prevent stdout buffering so logs appear instantly
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script
COPY weather_producer.py .

# Run the producer when container starts
CMD ["python", "weather_producer.py"]
