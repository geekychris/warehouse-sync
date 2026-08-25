package com.warehouse.sync.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FieldConfig {
    private String name;
    @JsonProperty("warehouse_name")
    private String warehouseName;
    private String type;
    private String description;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getWarehouseName() { return warehouseName; }
    public void setWarehouseName(String warehouseName) { this.warehouseName = warehouseName; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String effectiveWarehouseName() {
        return warehouseName == null ? name : warehouseName;
    }
}
