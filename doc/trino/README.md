# Trino Iceberg + Exasol Environment Setup

How to run a Trino server that queries **Iceberg** and **Exasol** data sources, and how to join tables across those catalogs. The setup uses:

- Iceberg catalog backed by **Nessie** + **MinIO**
- MinIO is a local S3-compatible object storage for Iceberg data and metadata files
- Nessie is an Iceberg Catalog with multi-table transaction support
- Exasol container, started by [ExasolIcebergQueryRunner](https://github.com/skyglass/trino/blob/demo/trino_iceberg_exasol/plugin/trino-exasol/src/test/java/io/trino/plugin/exasol/ExasolIcebergQueryRunner.java)
- Trino server, configured with Iceberg and Exasol connectors (started by [ExasolIcebergQueryRunner](https://github.com/skyglass/trino/blob/demo/trino_iceberg_exasol/plugin/trino-exasol/src/test/java/io/trino/plugin/exasol/ExasolIcebergQueryRunner.java)
- Example SQL schema and queries


1. Start Iceberg Nessie Catalog and MinIO
```shell
docker-compose up catalog -d
docker-compose up storage -d
docker-compose up mc -d
```

2. Clone Trino 477 demo branch: https://github.com/skyglass/trino/tree/demo/trino_iceberg_exasol
3. Build Trino with `mvn package`
4. Alternatively, if you already have Trino 477 or newer version:
* a. Download [ExasolIcebergQueryRunner](https://github.com/skyglass/trino/blob/demo/trino_iceberg_exasol/plugin/trino-exasol/src/test/java/io/trino/plugin/exasol/ExasolIcebergQueryRunner.java)
* b. Add ExasolIcebergQueryRunner to "**trino-exasol** test package
* c. Add "**trino-iceberg**" dependency to "**trino-exasol**" pom.xml
5. Start Exasol and Trino with [ExasolIcebergQueryRunner](https://github.com/skyglass/trino/blob/demo/trino_iceberg_exasol/plugin/trino-exasol/src/test/java/io/trino/plugin/exasol/ExasolIcebergQueryRunner.java)
6. Open SQL client with Trino and Exasol data sources (for example **DBeaver**)
7. Run SQL script to create tables in Iceberg and Exasol and insert test data (See **SQL Scripts** and **Notes on SQL Scripts**)
---

# Notes on SQL Scripts:
1. Iceberg and Exasol read-only queries can be executed with Trino SQL client
2. Exasol write queries can be executed with Exasol SQL client
3. Trino Exasol Connector is read-only!
4. Exasol CREATE TABLE and INSERT queries are not supported by Trino.
5. Use Exasol SQL client for CREATE TABLE and INSERT queries on Exasol database
6. Use Trino SQL client for CREATE TABLE and INSERT queries on Iceberg database
7. Use Trino SQL client for federated read-only queries on Iceberg and Exasol tables
---

## 📁 SQL scripts

```sql
-- 1. Iceberg side: create schema and table (Trino SQL client)
CREATE SCHEMA IF NOT EXISTS iceberg.hr;

CREATE TABLE iceberg.hr.employee (
id INT,
username VARCHAR,
hashtype_col VARBINARY
)
WITH (
format = 'PARQUET'
);

-- Insert sample data into Iceberg (Trino SQL client)
INSERT INTO iceberg.hr.employee (id, username, hashtype_col)
VALUES
(1, 'alice', from_hex('aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899')),
(2, 'bob',   from_hex('ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100'));

-- 2. Exasol side: create schema and table (Exasol SQL client)
CREATE SCHEMA IF NOT EXISTS hr;

CREATE TABLE hr.employee (
id DECIMAL(18,0),
username VARCHAR(100),
hashtype_col HASHTYPE(32 BYTE),
hashtype_col2 HASHTYPE(16 BYTE)
);

-- Insert sample data into Exasol (Exasol SQL Client)
INSERT INTO hr.employee (id, username, hashtype_col, hashtype_col2)
VALUES
(1, 'alice', 'aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899', '00112233445566778899aabbccddeeff'),
(2, 'bob',   'ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100', '8899aabbccddeeff0011223344556677');

-- 3. Cross-catalog join query (Trino SQL Client)
SELECT
i.id,
i.username AS iceberg_user,
e.username AS exasol_user,
i.hashtype_col AS iceberg_hash,
e.hashtype_col,
e.hashtype_col2
FROM iceberg.hr.employee i
JOIN exasol.hr.employee e
ON i.id = e.id;

-- 4. Cross-catalog join query with readable hashtype columns (Trino SQL Client)
SELECT
    i.id,
    i.username AS iceberg_user,
    e.username AS exasol_user,
    to_hex(i.hashtype_col) AS iceberg_hash_hex,
    to_hex(e.hashtype_col) AS exasol_hash_hex,
    to_hex(e.hashtype_col2) AS exasol_hash2_hex
FROM iceberg.hr.employee i
         JOIN exasol.hr.employee e
              ON i.id = e.id;

-- 5. Cross-catalog query with trino varbinary column joining on exasol hashtype column (Trino SQL Client)
SELECT
    i.id,
    i.username AS iceberg_user,
    e.username AS exasol_user,
    to_hex(i.hashtype_col) AS iceberg_hash_hex,
    to_hex(e.hashtype_col) AS exasol_hash_hex,
    to_hex(e.hashtype_col2) AS exasol_hash2_hex
FROM iceberg.hr.employee i
JOIN exasol.hr.employee e
ON i.hashtype_col = e.hashtype_col;


-- 6. Cross-catalog join with LIMIT  (Trino SQL Client)
SELECT
    i.id,
    i.username AS iceberg_user,
    e.username AS exasol_user,
    to_hex(e.hashtype_col) AS exasol_hash_hex,
    to_hex(e.hashtype_col2) AS exasol_hash2_hex

FROM iceberg.hr.employee i
         JOIN exasol.hr.employee e
              ON i.hashtype_col = e.hashtype_col
    LIMIT 1;


-- 7. Cross-catalog join with TOPN ordered by username ASC (Trino SQL Client)
SELECT
    i.id,
    i.username AS iceberg_user,
    e.username AS exasol_user,
    to_hex(i.hashtype_col) AS iceberg_hash_hex,
    to_hex(e.hashtype_col) AS exasol_hash_hex
FROM iceberg.hr.employee i
         JOIN exasol.hr.employee e
              ON i.id = e.id
ORDER BY i.username ASC
LIMIT 1;

-- 8. Cross-catalog join with TOPN ordered by username DESC (Trino SQL Client)
SELECT
    i.id,
    i.username AS iceberg_user,
    e.username AS exasol_user,
    to_hex(i.hashtype_col) AS iceberg_hash_hex,
    to_hex(e.hashtype_col) AS exasol_hash_hex
FROM iceberg.hr.employee i
         JOIN exasol.hr.employee e
              ON i.id = e.id
ORDER BY i.username DESC
    LIMIT 1;                 
```


# Creating a Data Lakehouse Evironment for Experimenting with Apache Iceberg Locally

**pre-requisites:**
- Docker Installed

## Setup

In your favorite IDE open up a an empty folder/workspace and create a `docker-compose.yaml` file with the following:

```yaml
###########################################
# Flink - Iceberg - Nessie Setup
###########################################

version: "3"

services:
  # Spark Notebook Server
  spark-iceberg:
    image: alexmerced/spark33-notebook
    container_name: spark-iceberg
    networks:
      iceberg-nessie-flink-net:
    depends_on:
      - catalog
      - storage
    volumes:
      - ./warehouse:/home/docker/warehouse
      - ./notebooks:/home/docker/notebooks
      - ./datasets:/home/docker/datasets
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
    ports:
      - 8888:8888
      - 8080:8080
      - 10000:10000
      - 10001:10001
  # Nessie Catalog Server Using In-Memory Store
  catalog:
    image: projectnessie/nessie:0.67.0
    container_name: catalog
    networks:
      iceberg-nessie-flink-net:
    ports:
      - 19120:19120
  # Minio Storage Server
  storage:
    image: minio/minio:RELEASE.2023-07-21T21-12-44Z
    container_name: storage
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=storage
      - MINIO_REGION_NAME=us-east-1
      - MINIO_REGION=us-east-1
    networks:
      iceberg-nessie-flink-net:
    ports:
      - 9001:9001
      - 9000:9000
    command: ["server", "/data", "--console-address", ":9001"]
  # Minio Client Container
  mc:
    depends_on:
      - storage
    image: minio/mc:RELEASE.2023-07-21T20-44-27Z
    container_name: mc
    networks:
      iceberg-nessie-flink-net:
        aliases:
          - minio.storage
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://storage:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc mb minio/iceberg;
      /usr/bin/mc policy set public minio/warehouse;
      /usr/bin/mc policy set public minio/iceberg;
      tail -f /dev/null
      "
  # Flink Job Manager
  flink-jobmanager:
    image: alexmerced/iceberg-flink-1.3.1
    ports:
      - "8081:8081"
    command: jobmanager
    networks:
      iceberg-nessie-flink-net:
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
      - S3_ENDPOINT=http://minio.storage:9000
      - S3_PATH_STYLE_ACCESS=true
  # Flink Task Manager
  flink-taskmanager:
    image: alexmerced/iceberg-flink-1.3.1
    depends_on:
      - flink-jobmanager
    command: taskmanager
    networks:
      iceberg-nessie-flink-net:
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 2
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
      - S3_ENDPOINT=http://minio.storage:9000
      - S3_PATH_STYLE_ACCESS=true
  # Dremio
  dremio:
    platform: linux/x86_64
    image: dremio/dremio-oss:latest
    ports:
      - 9047:9047
      - 31010:31010
      - 32010:32010
    container_name: dremio
    networks:
      iceberg-nessie-flink-net:
networks:
  iceberg-nessie-flink-net:
```

You can find an example at the follow repository:

[Example Data Engineer Dev Environment Repo](https://github.com/developer-advocacy-dremio/apache-iceberg-tutorial-environment)

## Start Up

Now follow these direction to start up each service (each command should be run in seperate terminal windows):


- `docker-compose up catalog` this will create a Nessie catalog server

- `docker-compose up storage` this will create a minio server accessible on localhost:9000

- `docker-compose up mc` this will start the minio client which will create our initial buckets

> Note that the `storage` container has folder `/minio_data` mapped to `storage` container volume. You can use this to share data with the container, as it can save files here that you can access and you can place files in the folder that the container will be able to access.

## Tear Down

- To turn off all the containers run `docker-compose down`

- To turn off one container at a time run `docker-compose down container_name` using the same names from the startup section.
