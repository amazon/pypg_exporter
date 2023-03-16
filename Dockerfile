FROM python:3.10-alpine

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY pypg_exporter.py .

# Expose port 8000
EXPOSE 8000

# Start the Prometheus exporter
CMD ["python", "pypg_exporter.py"]
