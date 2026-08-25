package com.warehouse.sync.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SourceConfig {
    private String type;
    private Map<String, Object> connection = Collections.emptyMap();
    private String schema;
    private String table;
    private List<String> columns = Collections.emptyList();
    @JsonProperty("primary_key")
    private List<String> primaryKey = Collections.emptyList();
    @JsonProperty("incremental_column")
    private String incrementalColumn;
    @JsonProperty("incremental_column_type")
    private String incrementalColumnType;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Map<String, Object> getConnection() { return connection; }
    public void setConnection(Map<String, Object> connection) {
        this.connection = connection == null ? Collections.emptyMap() : connection;
    }

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }

    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }

    public List<String> getColumns() { return columns; }
    public void setColumns(List<String> columns) {
        this.columns = columns == null ? Collections.emptyList() : columns;
    }

    public List<String> getPrimaryKey() { return primaryKey; }
    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey == null ? Collections.emptyList() : primaryKey;
    }

    public String getIncrementalColumn() { return incrementalColumn; }
    public void setIncrementalColumn(String incrementalColumn) { this.incrementalColumn = incrementalColumn; }

    public String getIncrementalColumnType() { return incrementalColumnType; }
    public void setIncrementalColumnType(String incrementalColumnType) { this.incrementalColumnType = incrementalColumnType; }

    /** Returns "schema.table" if schema is set, else just "table". */
    public String fullyQualifiedTable() {
        return (schema == null || schema.isEmpty()) ? table : schema + "." + table;
    }
}
