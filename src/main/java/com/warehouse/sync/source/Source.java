package com.warehouse.sync.source;

import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.state.Watermark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Source SPI.
 *
 * A Source knows how to read a batch of rows from some upstream system
 * (Postgres, MySQL, Debezium topic, ...) given a starting watermark and
 * a batch size. It returns a Spark Dataset<Row> ready for a WarehouseWriter.
 *
 * Implementations are looked up by their type discriminator (matched against
 * the config's {@code source.type} field) via {@link SourceFactoryRegistry},
 * which loads {@link SourceFactory} instances through ServiceLoader.
 *
 * Adding a new source type = new SourceFactory + META-INF/services entry.
 * No change to the engine.
 */
public interface Source extends AutoCloseable {

    /**
     * Read a single batch. Should return AT MOST batch_size rows with
     * {@code incremental_column > previousWatermark} ordered ascending
     * so the engine can advance the watermark to max(incremental_column).
     *
     * For full_replace mode the engine passes {@link Watermark#UNSET} and
     * expects the source to return the whole table.
     */
    SourceBatch readBatch(SparkSession spark, SyncConfig config, Watermark previousWatermark);

    @Override
    default void close() {}
}
