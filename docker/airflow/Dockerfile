# Airflow + Java + PySpark (spark-submit ở client mode) + PyIceberg (ghi Bronze)
FROM apache/airflow:2.10.5-python3.10
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ARG ICEBERG_VERSION=1.9.2
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/iceberg-spark-runtime.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
 && curl -fsSL -o /opt/spark-jars/iceberg-aws-bundle.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && chmod 644 /opt/spark-jars/*.jar
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
