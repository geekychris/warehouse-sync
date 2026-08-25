package com.warehouse.sync.writer;

import com.warehouse.sync.config.SyncConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Writes a batch of rows to the warehouse. Implementations correspond
 * 1:1 with {@link com.warehouse.sync.config.WriteMode}.
 *
 * Contract: rows have already been enriched with envelope metadata by
 * the engine (_sync_id, _sync_version, _ingested_at, _ingested_date,
 * _source_op, _batch_id). Writer decides how to persist them.
 */
public interface WarehouseWriter {
    /**
     * Ensure the target table exists with a schema compatible with the
     * given rows. May be called every run — idempotent.
     */
    void ensureTable(SparkSession spark, SyncConfig config, Dataset<Row> sample);

    /** Persist one batch. Rows already carry envelope metadata. */
    void write(SparkSession spark, SyncConfig config, Dataset<Row> rows);
}
