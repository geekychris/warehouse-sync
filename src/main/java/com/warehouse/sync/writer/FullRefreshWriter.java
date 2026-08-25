package com.warehouse.sync.writer;

import com.warehouse.sync.config.SyncConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replaces the target table's contents each run. Uses Iceberg's
 * {@code REPLACE TABLE ... AS SELECT} where supported (atomic swap), falling
 * back to CREATE OR REPLACE + INSERT OVERWRITE.
 *
 * Suitable for small reference tables where deletes matter but volume is
 * low. Not suitable for large fact tables — pair merge + periodic full
 * refresh instead.
 */
public class FullRefreshWriter implements WarehouseWriter {
    private static final Logger LOG = LoggerFactory.getLogger(FullRefreshWriter.class);

    @Override
    public void ensureTable(SparkSession spark, SyncConfig config, Dataset<Row> sample) {
        // Not needed — write() always CREATES OR REPLACES.
    }

    @Override
    public void write(SparkSession spark, SyncConfig config, Dataset<Row> rows) {
        Dataset<Row> out = WriterSupport.applyRenames(rows, config);
        String table = config.getWarehouse().catalogQualifiedTable();
        LOG.info("Full-replace: {} rows → {}", out.count(), table);
        out.writeTo(table).using("iceberg").createOrReplace();
    }
}
