package com.warehouse.sync.writer;

import com.warehouse.sync.config.WriteMode;

public final class WriterFactory {
    private WriterFactory() {}

    public static WarehouseWriter forMode(WriteMode mode) {
        switch (mode) {
            case APPEND_ONLY:  return new AppendOnlyWriter();
            case MERGE:        return new MergeWriter();
            case FULL_REPLACE: return new FullRefreshWriter();
            default: throw new IllegalStateException("unreachable: " + mode);
        }
    }
}
