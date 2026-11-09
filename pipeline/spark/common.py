"""SparkSession dùng chung, cấu hình catalog Iceberg tên `iceberg` (trùng tên catalog bên Trino)."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

CATALOG = "iceberg"


def get_spark(app_name: str) -> SparkSession:
    c = f"spark.sql.catalog.{CATALOG}"
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{c}.type", "rest")
        .config(f"{c}.uri", os.environ["ICEBERG_REST_URI"])
        .config(f"{c}.warehouse", os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/"))
        .config(f"{c}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{c}.s3.endpoint", os.environ["S3_ENDPOINT"])
        .config(f"{c}.s3.path-style-access", "true")
        .config(f"{c}.s3.access-key-id", os.environ["AWS_ACCESS_KEY_ID"])
        .config(f"{c}.s3.secret-access-key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(f"{c}.client.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
    )
    return builder.getOrCreate()
