FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py /app/

# Create necessary directories
RUN mkdir -p /app/data /app/chunks /app/intermediate /app/reducer_input /app/output

CMD ["python", "coordinator.py"]
