FROM python:3.11-slim

# System libraries (sasl) needed for PyHive to connect to the Hive/Spark Thrift
# Server over Thrift. Spark Thrift accepts NOSASL so the connection is simple,
# but thrift_sasl still has to compile.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libsasl2-dev libsasl2-modules && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

# Keep the container running idle; Claude Desktop starts the server in stdio
# mode via "docker exec -i". We use tail to idle-wait.
CMD ["tail", "-f", "/dev/null"]
