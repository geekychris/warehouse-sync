package com.warehouse.sync.state;

import java.time.Instant;
import java.util.UUID;

/** One row in {@code sync_state.run_history}. Immutable. */
public class RunRecord {
    public final String runId;
    public final String syncId;
    public final Instant startedAt;
    public final Instant finishedAt;
    public final long rowsSynced;
    public final String status;              // OK | FAILED | NOOP
    public final String errorMessage;        // null unless status=FAILED
    public final String watermarkBefore;     // string form; null if unset
    public final String watermarkAfter;      // string form; null if unset
    public final String mode;                // append_only | merge | full_replace

    public RunRecord(String runId, String syncId, Instant startedAt, Instant finishedAt,
                     long rowsSynced, String status, String errorMessage,
                     String watermarkBefore, String watermarkAfter, String mode) {
        this.runId = runId;
        this.syncId = syncId;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.rowsSynced = rowsSynced;
        this.status = status;
        this.errorMessage = errorMessage;
        this.watermarkBefore = watermarkBefore;
        this.watermarkAfter = watermarkAfter;
        this.mode = mode;
    }

    public static String newRunId() { return UUID.randomUUID().toString(); }
}
