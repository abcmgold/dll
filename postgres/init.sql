-- Một Postgres dùng chung cho: metadata Airflow, metadata Superset, Iceberg JDBC catalog
CREATE DATABASE airflow;
CREATE DATABASE superset;
CREATE DATABASE iceberg_catalog;
