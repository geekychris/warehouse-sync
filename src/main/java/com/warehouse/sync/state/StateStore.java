package com.warehouse.sync.state;

import java.time.Instant;
import java.util.List;

/**
 * Persistence for per-sync watermarks + run history.
 *
 * Two operations back the engine loop:
 *   - {@link #readWatermark} — before starting a batch
 *   - {@link #advanceWatermark} + {@link #recordRun} — after a batch commits
 *
 * Two operations back the debug CLIs:
 *   - {@link #inspect} — print current state for one or all syncs
 *   - {@link #setWatermark} — used by `backfill` to reset/move a watermark
 */
public interface StateStore extends AutoCloseable {

    Watermark readWatermark(String syncId, String wireType);

    void advanceWatermark(String syncId, Watermark newWatermark, Instant asOf);

    /** Blindly overwrite the watermark for one sync (used by `backfill`). */
    void setWatermark(String syncId, Watermark w, Instant asOf);

    void recordRun(RunRecord run);

    /** Return summary rows for all syncs (or one syncId if provided). */
    List<InspectRow> inspect(String syncIdOrNull, int recentRunLimit);

    /** Idempotently create the underlying tables if missing. */
    void ensureTables();

    @Override
    default void close() {}

    class InspectRow {
        public final String syncId;
        public final String watermarkValue;
        public final String watermarkType;
        public final Instant watermarkUpdatedAt;
        public final List<RunRecord> recentRuns;

        public InspectRow(String syncId, String watermarkValue, String watermarkType,
                          Instant watermarkUpdatedAt, List<RunRecord> recentRuns) {
            this.syncId = syncId;
            this.watermarkValue = watermarkValue;
            this.watermarkType = watermarkType;
            this.watermarkUpdatedAt = watermarkUpdatedAt;
            this.recentRuns = recentRuns;
        }
    }
}
