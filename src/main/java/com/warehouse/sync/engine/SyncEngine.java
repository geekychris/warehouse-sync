package com.warehouse.sync.engine;

import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.config.WriteMode;
import com.warehouse.sync.envelope.SyncEnvelope;
import com.warehouse.sync.source.Source;
import com.warehouse.sync.source.SourceBatch;
import com.warehouse.sync.source.SourceFactoryRegistry;
import com.warehouse.sync.state.RunRecord;
import com.warehouse.sync.state.StateStore;
import com.warehouse.sync.state.Watermark;
import com.warehouse.sync.writer.WarehouseWriter;
import com.warehouse.sync.writer.WriterFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.apache.spark.sql.functions.lit;

/**
 * Orchestrates one run of one sync config.
 *
 * Loop:
 *   1. Read watermark from state store.
 *   2. Ask Source for a batch starting after that watermark.
 *   3. If empty → record NOOP run, exit.
 *   4. Add envelope columns, ensure target table, call writer.
 *   5. Advance watermark. Record run. Loop back to step 2 unless
 *      returned rows < batch_size (drained).
 *
 * The engine is stateless — one instance per JVM is fine, or one per sync
 * inside `serve`.
 */
public class SyncEngine {
    private static final Logger LOG = LoggerFactory.getLogger(SyncEngine.class);

    private final SparkSession spark;
    private final StateStore stateStore;

    public SyncEngine(SparkSession spark, StateStore stateStore) {
        this.spark = spark;
        this.stateStore = stateStore;
    }

    /** Force a full_replace this run regardless of the config's declared mode. */
    public SyncResult runFullRefresh(SyncConfig cfg) {
        SyncConfig forced = shallowCopyWithMode(cfg, WriteMode.FULL_REPLACE);
        return run(forced);
    }

    public SyncResult run(SyncConfig cfg) {
        Instant startedAt = Instant.now();
        String runId = UUID.randomUUID().toString();
        stateStore.ensureTables();

        String wireType = cfg.getSource().getIncrementalColumnType();
        Watermark before = (cfg.getMode() == WriteMode.FULL_REPLACE)
                ? Watermark.UNSET
                : stateStore.readWatermark(cfg.getSyncId(), wireType);
        LOG.info("[{}] run={} mode={} watermark_before={}", cfg.getSyncId(), runId, cfg.getMode(), before);

        Source source = SourceFactoryRegistry.instance().get(cfg.getSource().getType()).create(cfg);
        WarehouseWriter writer = WriterFactory.forMode(cfg.getMode());

        long totalRows = 0;
        int batchNum = 0;
        Watermark watermark = before;

        try (Source s = source) {
            while (true) {
                batchNum++;
                SourceBatch batch = s.readBatch(spark, cfg, watermark);
                if (batch.isEmpty()) {
                    if (batchNum == 1) {
                        LOG.info("[{}] no new rows since {}", cfg.getSyncId(), watermark);
                        stateStore.recordRun(new RunRecord(runId, cfg.getSyncId(), startedAt, Instant.now(),
                                0, "NOOP", null, wmString(before), wmString(before), cfg.getMode().wire()));
                        return new SyncResult(cfg.getSyncId(), 0, 0, before, before,
                                Duration.between(startedAt, Instant.now()), "NOOP", null);
                    }
                    break;
                }
                Dataset<Row> enriched = enrich(batch.rows(), cfg, runId);
                if (batchNum == 1) {
                    ensureNamespace(spark, cfg);
                    writer.ensureTable(spark, cfg, enriched);
                }
                writer.write(spark, cfg, enriched);

                watermark = batch.nextWatermark();
                if (!watermark.isUnset()) {
                    stateStore.advanceWatermark(cfg.getSyncId(), watermark, Instant.now());
                }
                totalRows += batch.rowCount();

                // Loop-until-drained: keep pulling until a batch comes back
                // smaller than batch_size (last page) or the source has no
                // watermark (full_replace — one big batch).
                if (cfg.getMode() == WriteMode.FULL_REPLACE) break;
                if (batch.rowCount() < cfg.getSchedule().getBatchSize()) break;
            }
        } catch (Exception e) {
            LOG.error("[{}] sync failed", cfg.getSyncId(), e);
            stateStore.recordRun(new RunRecord(runId, cfg.getSyncId(), startedAt, Instant.now(),
                    totalRows, "FAILED", e.getMessage(), wmString(before), wmString(watermark), cfg.getMode().wire()));
            return new SyncResult(cfg.getSyncId(), totalRows, batchNum, before, watermark,
                    Duration.between(startedAt, Instant.now()), "FAILED", e.getMessage());
        }

        stateStore.recordRun(new RunRecord(runId, cfg.getSyncId(), startedAt, Instant.now(),
                totalRows, "OK", null, wmString(before), wmString(watermark), cfg.getMode().wire()));
        return new SyncResult(cfg.getSyncId(), totalRows, batchNum, before, watermark,
                Duration.between(startedAt, Instant.now()), "OK", null);
    }

    /** Attach envelope metadata (sync_id, version, ingest_ts, ingest_date, op, batch_id). */
    private Dataset<Row> enrich(Dataset<Row> raw, SyncConfig cfg, String batchId) {
        String op = cfg.getMode() == WriteMode.FULL_REPLACE
                ? SyncEnvelope.OP_SNAPSHOT : SyncEnvelope.OP_UPSERT;
        return raw
                .withColumn(SyncEnvelope.SYNC_ID,       lit(cfg.getSyncId()))
                .withColumn(SyncEnvelope.SYNC_VERSION,  lit(cfg.getVersion()))
                .withColumn(SyncEnvelope.INGESTED_AT,   lit(new java.sql.Timestamp(System.currentTimeMillis())))
                .withColumn(SyncEnvelope.INGESTED_DATE, lit(java.sql.Date.valueOf(LocalDate.now(ZoneOffset.UTC))))
                .withColumn(SyncEnvelope.SOURCE_OP,     lit(op))
                .withColumn(SyncEnvelope.BATCH_ID,      lit(batchId));
    }

    /**
     * Ensure the target namespace (e.g. `iceberg.warehouse`) exists so
     * subsequent CREATE TABLE calls don't fail. Idempotent.
     */
    private static void ensureNamespace(SparkSession spark, SyncConfig cfg) {
        String qualifiedTable = cfg.getWarehouse().catalogQualifiedTable();
        int lastDot = qualifiedTable.lastIndexOf('.');
        if (lastDot > 0) {
            String namespace = qualifiedTable.substring(0, lastDot);
            spark.sql("CREATE NAMESPACE IF NOT EXISTS " + namespace);
        }
    }

    private static String wmString(Watermark w) {
        return w == null || w.isUnset() ? null : w.value();
    }

    private static SyncConfig shallowCopyWithMode(SyncConfig cfg, WriteMode mode) {
        SyncConfig c = new SyncConfig();
        c.setName(cfg.getName());
        c.setSyncId(cfg.getSyncId());
        c.setVersion(cfg.getVersion());
        c.setDescription(cfg.getDescription());
        c.setSource(cfg.getSource());
        c.setMode(mode);
        c.setWarehouse(cfg.getWarehouse());
        c.setSchedule(cfg.getSchedule());
        c.setFields(cfg.getFields());
        return c;
    }
}
