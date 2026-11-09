# Iceberg REST Catalog, lưu metadata catalog trong Postgres.
# Image gốc chỉ có driver SQLite nên bổ sung driver PostgreSQL vào classpath.
FROM apache/iceberg-rest-fixture:1.9.2
USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /usr/lib/iceberg-rest/postgresql.jar
RUN chmod 644 /usr/lib/iceberg-rest/postgresql.jar
USER iceberg:iceberg
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
