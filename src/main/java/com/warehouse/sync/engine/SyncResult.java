package com.warehouse.sync.engine;

import com.warehouse.sync.state.Watermark;

import java.time.Duration;

public class SyncResult {
    public final String syncId;
    public final long rowsSynced;
    public final int batches;
    public final Watermark watermarkBefore;
    public final Watermark watermarkAfter;
    public final Duration duration;
    public final String status;         // OK | FAILED | NOOP
    public final String error;

    public SyncResult(String syncId, long rowsSynced, int batches,
                      Watermark watermarkBefore, Watermark watermarkAfter,
                      Duration duration, String status, String error) {
        this.syncId = syncId;
        this.rowsSynced = rowsSynced;
        this.batches = batches;
        this.watermarkBefore = watermarkBefore;
        this.watermarkAfter = watermarkAfter;
        this.duration = duration;
        this.status = status;
        this.error = error;
    }

    @Override
    public String toString() {
        return String.format("[%s] status=%s rows=%d batches=%d watermark=%s→%s took=%s%s",
                syncId, status, rowsSynced, batches, watermarkBefore, watermarkAfter,
                duration, error == null ? "" : (" error=" + error));
    }
}
