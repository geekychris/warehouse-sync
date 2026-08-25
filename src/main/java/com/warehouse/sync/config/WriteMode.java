package com.warehouse.sync.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum WriteMode {
    APPEND_ONLY("append_only"),
    MERGE("merge"),
    FULL_REPLACE("full_replace");

    private final String wire;

    WriteMode(String wire) {
        this.wire = wire;
    }

    @JsonValue
    public String wire() {
        return wire;
    }

    @JsonCreator
    public static WriteMode fromWire(String s) {
        for (WriteMode m : values()) {
            if (m.wire.equals(s)) return m;
        }
        throw new IllegalArgumentException("Unknown write mode: " + s);
    }
}
