package com.warehouse.sync.spark;

import org.apache.spark.sql.SparkSession;

/**
 * Builds the SparkSession used by run/dry-run/serve/inspect/backfill. All
 * these subcommands need the same catalog + S3A wiring, so it lives here
 * once.
 *
 * Uses env vars so the same jar runs against the local lakehouse
 * (localhost:18013 etc.) or a real cluster without recompilation:
 *
 *   WAREHOUSE_SYNC_CATALOG            catalog name (default: iceberg)
 *   WAREHOUSE_SYNC_WAREHOUSE_PATH     s3a://warehouse (default)
 *   WAREHOUSE_SYNC_METASTORE_URI      thrift://hive-metastore:9083 (default)
 *   WAREHOUSE_SYNC_S3_ENDPOINT        http://minio:9000 (default)
 *   WAREHOUSE_SYNC_S3_ACCESS_KEY      admin
 *   WAREHOUSE_SYNC_S3_SECRET_KEY      password123
 */
public final class SparkSessionFactory {
    private SparkSessionFactory() {}

    public static SparkSession build(String appName) {
        String catalog       = env("WAREHOUSE_SYNC_CATALOG",        "iceberg");
        String warehousePath = env("WAREHOUSE_SYNC_WAREHOUSE_PATH", "s3a://warehouse");
        String metastoreUri  = env("WAREHOUSE_SYNC_METASTORE_URI",  "thrift://localhost:18083");
        String s3Endpoint    = env("WAREHOUSE_SYNC_S3_ENDPOINT",    "http://localhost:18000");
        String s3AccessKey   = env("WAREHOUSE_SYNC_S3_ACCESS_KEY",  "admin");
        String s3SecretKey   = env("WAREHOUSE_SYNC_S3_SECRET_KEY",  "password123");

        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master(env("WAREHOUSE_SYNC_SPARK_MASTER", "local[*]"))
                // Pin session timezone to UTC. The JDBC datasource propagates the
                // Spark session timezone to the source Postgres via SET TIME ZONE;
                // Postgres rejects Java-style zone names like "US/Pacific", so we
                // sidestep by staying in UTC.
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog." + catalog,
                        "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + catalog + ".type", "hive")
                .config("spark.sql.catalog." + catalog + ".uri", metastoreUri)
                .config("spark.sql.catalog." + catalog + ".warehouse", warehousePath)
                // Default HadoopFileIO uses fs.s3a.* config below — fine against MinIO
                // and doesn't need the AWS SDK v2 that S3FileIO would drag in.
                .config("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
                .config("spark.hadoop.fs.s3a.access.key", s3AccessKey)
                .config("spark.hadoop.fs.s3a.secret.key", s3SecretKey)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        spark.sparkContext().setLogLevel(env("WAREHOUSE_SYNC_LOG_LEVEL", "WARN"));
        return spark;
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}
