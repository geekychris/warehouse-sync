package com.warehouse.sync.writer;

import com.warehouse.sync.config.FieldConfig;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.envelope.SyncEnvelope;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared helpers used by all three writers: table DDL, column renaming, etc. */
final class WriterSupport {
    private WriterSupport() {}

    /**
     * Build CREATE TABLE IF NOT EXISTS SQL by inspecting the sample Dataset's
     * schema. The engine passes a small sample from the first batch — sufficient
     * for column types.
     */
    static void ensureTable(SparkSession spark, SyncConfig cfg, Dataset<Row> sample) {
        String table = cfg.getWarehouse().catalogQualifiedTable();
        Map<String, String> fieldOverrides = fieldOverrides(cfg);

        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(table).append(" (\n");
        List<String> cols = new ArrayList<>();
        for (StructField f : sample.schema().fields()) {
            String warehouseName = fieldOverrides.getOrDefault(f.name(), f.name());
            String type = fieldOverrides.containsKey(f.name() + "|type")
                    ? fieldOverrides.get(f.name() + "|type")
                    : sparkTypeToIceberg(f.dataType());
            cols.add("  " + quoteIdent(warehouseName) + " " + type);
        }
        ddl.append(String.join(",\n", cols));
        ddl.append("\n) USING iceberg");

        if (!cfg.getWarehouse().getPartitionBy().isEmpty()) {
            ddl.append(" PARTITIONED BY (")
                    .append(String.join(", ", cfg.getWarehouse().getPartitionBy()))
                    .append(")");
        }
        spark.sql(ddl.toString());
    }

    /**
     * Rename source→warehouse columns per config, if any FieldConfig entries
     * have a warehouse_name override. Returns a Dataset with renamed columns.
     */
    static Dataset<Row> applyRenames(Dataset<Row> rows, SyncConfig cfg) {
        Dataset<Row> out = rows;
        for (FieldConfig f : cfg.getFields()) {
            if (f.getWarehouseName() != null && !f.getWarehouseName().equals(f.getName())) {
                out = out.withColumnRenamed(f.getName(), f.getWarehouseName());
            }
        }
        return out;
    }

    /** Iceberg SQL identifier quoting (backticks are illegal; use no-quote or lowercase). */
    static String quoteIdent(String s) { return s; }

    /** Rough Spark → Iceberg SQL type map. Falls back to STRING for unknown. */
    static String sparkTypeToIceberg(DataType dt) {
        String simple = dt.simpleString();
        switch (simple.toLowerCase()) {
            case "int":       return "INT";
            case "bigint":    return "BIGINT";
            case "smallint":  return "SMALLINT";
            case "tinyint":   return "TINYINT";
            case "double":    return "DOUBLE";
            case "float":     return "FLOAT";
            case "boolean":   return "BOOLEAN";
            case "date":      return "DATE";
            case "timestamp": return "TIMESTAMP";
            case "string":    return "STRING";
            case "binary":    return "BINARY";
            default:
                if (simple.startsWith("decimal(")) return simple.toUpperCase();
                if (simple.startsWith("array<"))   return simple; // best-effort
                if (simple.startsWith("map<"))     return simple;
                if (simple.startsWith("struct<"))  return simple;
                return "STRING";
        }
    }

    private static Map<String, String> fieldOverrides(SyncConfig cfg) {
        Map<String, String> m = new LinkedHashMap<>();
        for (FieldConfig f : cfg.getFields()) {
            if (f.getWarehouseName() != null) m.put(f.getName(), f.getWarehouseName());
            if (f.getType() != null) m.put(f.getName() + "|type", f.getType());
        }
        return m;
    }

    /** Envelope columns that are added by the engine, in declaration order. */
    static List<String> envelopeColumns() {
        List<String> l = new ArrayList<>();
        l.add(SyncEnvelope.SYNC_ID);
        l.add(SyncEnvelope.SYNC_VERSION);
        l.add(SyncEnvelope.INGESTED_AT);
        l.add(SyncEnvelope.INGESTED_DATE);
        l.add(SyncEnvelope.SOURCE_OP);
        l.add(SyncEnvelope.BATCH_ID);
        return l;
    }

    /** Verify the sample's schema doesn't drift too far from the target — cheap sanity check. */
    static void warnOnDrift(SparkSession spark, String qualifiedTable, StructType sampleSchema) {
        try {
            StructType tableSchema = spark.table(qualifiedTable).schema();
            for (StructField f : sampleSchema.fields()) {
                if (java.util.Arrays.stream(tableSchema.fields()).noneMatch(t -> t.name().equalsIgnoreCase(f.name()))) {
                    System.err.println("[schema-drift] source column '" + f.name() + "' has no matching target column in " + qualifiedTable);
                }
            }
        } catch (Exception ignored) {
            // Table doesn't exist yet — normal on first run, no-op.
        }
    }
}
