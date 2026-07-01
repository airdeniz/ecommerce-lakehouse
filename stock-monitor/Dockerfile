# The stock-monitor consumes Kafka with kafka-python (unchanged), but it ALSO
# persists low-stock alerts to the Iceberg table lakehouse.ops.stock_alerts. To
# write Iceberg into the JDBC catalog on MinIO it needs a local Spark engine plus
# the Iceberg / hadoop-aws / postgresql JARs — the same set pyspark/Dockerfile
# bakes. So we start from the official Spark image rather than a plain Python one.
FROM apache/spark:3.5.1

USER root

# 1) kafka-python for the (unchanged) consumer loop.
# 2) The JARs Spark core does not ship, baked in for offline/reproducible starts:
#    - iceberg-spark-runtime      : read/write the Iceberg table format
#    - aws-java-sdk-bundle + hadoop-aws : reach MinIO over the S3A protocol
#    - postgresql                 : JDBC catalog connection to iceberg-db
# NOTE: the Kafka *Spark* connector JARs are intentionally NOT included — this
# service consumes via kafka-python, not Spark structured streaming.
RUN pip install --no-cache-dir kafka-python==2.0.2 && \
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

WORKDIR /app
COPY stock_monitor.py .

# Run via spark-submit so PySpark + its py4j gateway are on the path (the Spark
# image does not expose pyspark to a plain `python3`). kafka-python is available
# under the same interpreter. The SparkSession is still built lazily on the first
# alert, so the JVM only starts when an alert is actually persisted.
CMD ["/opt/spark/bin/spark-submit", "/app/stock_monitor.py"]
