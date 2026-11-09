# Spark standalone (master/worker) + thư viện Iceberg để đọc/ghi bảng trên MinIO
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu
USER root
ARG ICEBERG_VERSION=1.9.2
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/iceberg-*.jar
USER spark
