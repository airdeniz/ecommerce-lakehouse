# Start from Apache's official Spark image. It ships the Spark engine, JVM,
# PySpark and the base JARs, so we don't install Spark from scratch.
FROM apache/spark:3.5.1

# Switch to root for the install steps (installing libraries and downloading
# files needs privileges). The Spark image normally runs as the unprivileged
# 'spark' user; we switch back to it once the setup is done.
USER root

# Two things happen here:
# 1) pip installs the Kafka client library on the Python side.
# 2) We download JARs that are NOT in the Spark core from the Maven repo into
#    /opt/spark/jars/. Without these, Spark core does not "know" Kafka, Iceberg
#    or S3/MinIO. The groups:
#
#    - spark-sql-kafka + kafka-clients + token-provider + commons-pool2:
#        so Spark can read a stream from Kafka (the pipeline's input).
#    - iceberg-spark-runtime:
#        so Spark can read/write the Iceberg table format (bronze).
#    - aws-java-sdk-bundle + hadoop-aws:
#        so Spark can reach MinIO over the S3 protocol (storage).
#    - postgresql:
#        so Spark can connect to Postgres over JDBC.
#
# We BAKE the JARs into the image instead of fetching them at runtime with
# '--packages', so the container starts offline, reproducibly and fast.
RUN pip install --no-cache-dir kafka-python==2.0.2 && \
    curl -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.661.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.661/aws-java-sdk-bundle-1.12.661.jar && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# Setup done; switch back to the unprivileged 'spark' user for security.
USER spark

# Set the working directory and copy our own code (orders_stream.py) into the image.
WORKDIR /app
COPY . .
