package com.warehouse.sync.source;

import com.warehouse.sync.state.Watermark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Result of one {@link Source#readBatch} call.
 *
 * The engine uses {@link #rowCount} to decide whether to loop for another
 * batch, and {@link #nextWatermark} to advance the state store atomically
 * after the writer commits.
 */
public class SourceBatch {
    private final Dataset<Row> rows;
    private final long rowCount;
    private final Watermark nextWatermark;

    public SourceBatch(Dataset<Row> rows, long rowCount, Watermark nextWatermark) {
        this.rows = rows;
        this.rowCount = rowCount;
        this.nextWatermark = nextWatermark;
    }

    public Dataset<Row> rows() { return rows; }
    public long rowCount() { return rowCount; }
    public Watermark nextWatermark() { return nextWatermark; }

    public boolean isEmpty() { return rowCount == 0; }
}
