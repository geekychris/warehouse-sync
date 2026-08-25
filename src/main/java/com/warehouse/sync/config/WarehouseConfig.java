package com.warehouse.sync.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class WarehouseConfig {
    @JsonProperty("table_name")
    private String tableName;
    @JsonProperty("target_catalog")
    private String targetCatalog = "iceberg";
    @JsonProperty("partition_by")
    private List<String> partitionBy = Collections.emptyList();
    @JsonProperty("sort_by")
    private List<String> sortBy = Collections.emptyList();

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getTargetCatalog() { return targetCatalog; }
    public void setTargetCatalog(String targetCatalog) {
        this.targetCatalog = targetCatalog == null ? "iceberg" : targetCatalog;
    }

    public List<String> getPartitionBy() { return partitionBy; }
    public void setPartitionBy(List<String> partitionBy) {
        this.partitionBy = partitionBy == null ? Collections.emptyList() : partitionBy;
    }

    public List<String> getSortBy() { return sortBy; }
    public void setSortBy(List<String> sortBy) {
        this.sortBy = sortBy == null ? Collections.emptyList() : sortBy;
    }

    /** Fully-qualified name with catalog prefix (e.g. iceberg.warehouse.public_users). */
    public String catalogQualifiedTable() {
        return targetCatalog + "." + tableName;
    }
}
