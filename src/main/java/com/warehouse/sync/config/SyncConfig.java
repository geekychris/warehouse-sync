package com.warehouse.sync.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = false)
public class SyncConfig {
    private String name;
    @JsonProperty("sync_id")
    private String syncId;
    private String version;
    private String description;

    private SourceConfig source;
    private WriteMode mode;
    private WarehouseConfig warehouse;
    private ScheduleConfig schedule;
    private List<FieldConfig> fields = Collections.emptyList();

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getSyncId() { return syncId; }
    public void setSyncId(String syncId) { this.syncId = syncId; }

    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public SourceConfig getSource() { return source; }
    public void setSource(SourceConfig source) { this.source = source; }

    public WriteMode getMode() { return mode; }
    public void setMode(WriteMode mode) { this.mode = mode; }

    public WarehouseConfig getWarehouse() { return warehouse; }
    public void setWarehouse(WarehouseConfig warehouse) { this.warehouse = warehouse; }

    public ScheduleConfig getSchedule() { return schedule; }
    public void setSchedule(ScheduleConfig schedule) { this.schedule = schedule; }

    public List<FieldConfig> getFields() { return fields; }
    public void setFields(List<FieldConfig> fields) {
        this.fields = fields == null ? Collections.emptyList() : fields;
    }

    /**
     * Structural validation that JSON Schema can't easily express:
     * mode-specific requirements (merge needs PK, append_only needs
     * incremental column, etc.).
     */
    public void validateStructural() {
        if (source == null) throw new IllegalArgumentException("source is required");
        if (warehouse == null) throw new IllegalArgumentException("warehouse is required");
        if (schedule == null) throw new IllegalArgumentException("schedule is required");
        if (mode == null) throw new IllegalArgumentException("mode is required");

        switch (mode) {
            case MERGE:
                require(!source.getPrimaryKey().isEmpty(),
                        "mode=merge requires source.primary_key to be set");
                require(source.getIncrementalColumn() != null,
                        "mode=merge requires source.incremental_column to be set");
                break;
            case APPEND_ONLY:
                require(source.getIncrementalColumn() != null,
                        "mode=append_only requires source.incremental_column to be set");
                break;
            case FULL_REPLACE:
                // No PK or watermark required — the writer swaps the whole table.
                break;
        }
    }

    private static void require(boolean cond, String msg) {
        if (!cond) throw new IllegalArgumentException(msg);
    }
}
