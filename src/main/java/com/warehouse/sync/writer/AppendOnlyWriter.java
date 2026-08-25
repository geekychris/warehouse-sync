package com.warehouse.sync.writer;

import com.warehouse.sync.config.SyncConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Each batch is appended as-is. Cheapest writer; queries deduplicate downstream. */
public class AppendOnlyWriter implements WarehouseWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyWriter.class);

    @Override
    public void ensureTable(SparkSession spark, SyncConfig config, Dataset<Row> sample) {
        WriterSupport.ensureTable(spark, config, sample);
    }

    @Override
    public void write(SparkSession spark, SyncConfig config, Dataset<Row> rows) {
        Dataset<Row> out = WriterSupport.applyRenames(rows, config);
        String table = config.getWarehouse().catalogQualifiedTable();
        WriterSupport.warnOnDrift(spark, table, out.schema());
        LOG.info("Append-only write: {} rows → {}", out.count(), table);
        try {
            out.writeTo(table).append();
        } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
            throw new IllegalStateException(
                    "Target table missing at write time: " + table
                            + " — ensureTable() should have created it. This is a bug.", e);
        }
    }
}
