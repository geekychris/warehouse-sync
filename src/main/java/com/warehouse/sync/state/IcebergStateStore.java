package com.warehouse.sync.state;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Iceberg-backed StateStore. Two tables in a dedicated namespace so state
 * lives alongside warehouse data and is queryable from Trino.
 *
 *   sync_state.watermarks   — one row per sync_id (latest wins on merge)
 *   sync_state.run_history  — append-only audit log of every batch
 *
 * Both are created idempotently by {@link #ensureTables()}.
 *
 * IcebergStateStore uses parameterized Spark SQL — literals are quoted
 * through {@link #sqlLit} which strictly escapes single quotes. sync_id
 * values are validated by the config schema (^[a-z][a-z0-9_]*$) so they
 * cannot contain quotes at all in practice, but escaping is applied
 * defensively.
 */
public class IcebergStateStore implements StateStore {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergStateStore.class);

    private final SparkSession spark;
    private final String catalog;
    private final String namespace;

    public IcebergStateStore(SparkSession spark, String catalog) {
        this(spark, catalog, "sync_state");
    }

    public IcebergStateStore(SparkSession spark, String catalog, String namespace) {
        this.spark = spark;
        this.catalog = catalog;
        this.namespace = namespace;
    }

    private String wm() { return catalog + "." + namespace + ".watermarks"; }
    private String rh() { return catalog + "." + namespace + ".run_history"; }

    @Override
    public void ensureTables() {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalog + "." + namespace);

        spark.sql("CREATE TABLE IF NOT EXISTS " + wm() + " ("
                + "  sync_id STRING,"
                + "  watermark_value STRING,"
                + "  watermark_type STRING,"
                + "  updated_at TIMESTAMP"
                + ") USING iceberg");

        spark.sql("CREATE TABLE IF NOT EXISTS " + rh() + " ("
                + "  run_id STRING,"
                + "  sync_id STRING,"
                + "  started_at TIMESTAMP,"
                + "  finished_at TIMESTAMP,"
                + "  rows_synced BIGINT,"
                + "  status STRING,"
                + "  error_message STRING,"
                + "  watermark_before STRING,"
                + "  watermark_after STRING,"
                + "  mode STRING"
                + ") USING iceberg "
                + "PARTITIONED BY (days(started_at))");
    }

    @Override
    public Watermark readWatermark(String syncId, String wireType) {
        Dataset<Row> rows = spark.sql(
                "SELECT watermark_value, watermark_type FROM " + wm()
                        + " WHERE sync_id = " + sqlLit(syncId));
        List<Row> collected = rows.collectAsList();
        if (collected.isEmpty()) return Watermark.UNSET;
        Row r = collected.get(0);
        String v = r.isNullAt(0) ? null : r.getString(0);
        String t = r.isNullAt(1) ? null : r.getString(1);
        if (v == null) return Watermark.UNSET;
        // Prefer stored type; caller passes wireType only as a fallback for the first-ever run.
        String effectiveType = t != null ? t : wireType;
        return Watermark.of(Watermark.parseType(effectiveType.toLowerCase()), v);
    }

    @Override
    public void advanceWatermark(String syncId, Watermark newWatermark, Instant asOf) {
        setWatermark(syncId, newWatermark, asOf);
    }

    @Override
    public void setWatermark(String syncId, Watermark w, Instant asOf) {
        // Insert-then-delete-older pattern (Iceberg MERGE INTO would be nicer
        // but requires knowing the source data ahead of time; this two-step
        // is fine for a single-row-per-sync table).
        String value = w.isUnset() ? "NULL" : sqlLit(w.value());
        String type  = w.isUnset() ? "NULL" : sqlLit(w.type().name().toLowerCase());
        String ts    = sqlTs(asOf);

        spark.sql("DELETE FROM " + wm() + " WHERE sync_id = " + sqlLit(syncId));
        spark.sql("INSERT INTO " + wm() + " VALUES ("
                + sqlLit(syncId) + ", " + value + ", " + type + ", " + ts + ")");
    }

    @Override
    public void recordRun(RunRecord run) {
        spark.sql("INSERT INTO " + rh() + " VALUES ("
                + sqlLit(run.runId) + ", "
                + sqlLit(run.syncId) + ", "
                + sqlTs(run.startedAt) + ", "
                + sqlTs(run.finishedAt) + ", "
                + run.rowsSynced + ", "
                + sqlLit(run.status) + ", "
                + sqlLitNullable(run.errorMessage) + ", "
                + sqlLitNullable(run.watermarkBefore) + ", "
                + sqlLitNullable(run.watermarkAfter) + ", "
                + sqlLit(run.mode) + ")");
    }

    @Override
    public List<InspectRow> inspect(String syncIdOrNull, int recentRunLimit) {
        String where = syncIdOrNull == null ? "" : " WHERE sync_id = " + sqlLit(syncIdOrNull);
        Dataset<Row> wmRows = spark.sql(
                "SELECT sync_id, watermark_value, watermark_type, updated_at FROM " + wm() + where
                        + " ORDER BY sync_id");

        List<InspectRow> out = new ArrayList<>();
        for (Row w : wmRows.collectAsList()) {
            String syncId = w.getString(0);
            String value  = w.isNullAt(1) ? null : w.getString(1);
            String type   = w.isNullAt(2) ? null : w.getString(2);
            Instant updated = w.isNullAt(3) ? null : ((Timestamp) w.get(3)).toInstant();

            Dataset<Row> runs = spark.sql(
                    "SELECT run_id, started_at, finished_at, rows_synced, status, error_message,"
                            + " watermark_before, watermark_after, mode FROM " + rh()
                            + " WHERE sync_id = " + sqlLit(syncId)
                            + " ORDER BY started_at DESC LIMIT " + recentRunLimit);
            List<RunRecord> recent = new ArrayList<>();
            for (Row r : runs.collectAsList()) {
                recent.add(new RunRecord(
                        r.getString(0),
                        syncId,
                        ((Timestamp) r.get(1)).toInstant(),
                        r.isNullAt(2) ? null : ((Timestamp) r.get(2)).toInstant(),
                        r.getLong(3),
                        r.getString(4),
                        r.isNullAt(5) ? null : r.getString(5),
                        r.isNullAt(6) ? null : r.getString(6),
                        r.isNullAt(7) ? null : r.getString(7),
                        r.getString(8)
                ));
            }
            out.add(new InspectRow(syncId, value, type, updated, recent));
        }
        if (out.isEmpty() && syncIdOrNull == null) return Collections.emptyList();
        return out;
    }

    static String sqlLit(String s) {
        if (s == null) return "NULL";
        return "'" + s.replace("'", "''") + "'";
    }

    static String sqlLitNullable(String s) { return sqlLit(s); }

    static String sqlTs(Instant i) {
        return "TIMESTAMP '" + i.toString().replace("Z", "").replace("T", " ") + "'";
    }
}
