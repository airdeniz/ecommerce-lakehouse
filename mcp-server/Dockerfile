FROM python:3.11-slim

# PyHive'in Thrift uzerinden Hive/Spark Thrift Server'a baglanmasi icin
# gereken sistem kutuphaneleri (sasl). Spark Thrift NOSASL kabul ettiginden
# baglanti basittir, ama thrift_sasl yine de derlenebilmeli.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libsasl2-dev libsasl2-modules && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .

# Container surekli ayakta dursun; Claude Desktop "docker exec -i" ile
# server'i stdio modunda baslatacak. Idle beklemesi icin tail kullaniyoruz.
CMD ["tail", "-f", "/dev/null"]
