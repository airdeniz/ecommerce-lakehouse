# Unlike stock-monitor (which also writes Iceberg and therefore needs Spark), this
# service only consumes Kafka and writes Redis, so a plain Python base is enough.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aggregator.py .

CMD ["python", "aggregator.py"]
