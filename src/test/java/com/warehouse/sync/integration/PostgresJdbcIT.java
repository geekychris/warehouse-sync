package com.warehouse.sync.integration;

import com.warehouse.sync.config.ConfigLoader;
import com.warehouse.sync.config.ScheduleConfig;
import com.warehouse.sync.config.SourceConfig;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.config.WarehouseConfig;
import com.warehouse.sync.config.WriteMode;
import com.warehouse.sync.source.postgres.PostgresSource;
import com.warehouse.sync.source.SourceBatch;
import com.warehouse.sync.state.Watermark;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test — proves the PostgresSource actually reads rows from a
 * live Postgres via JDBC + Spark, using testcontainers.
 *
 * This test does NOT touch the warehouse (no MinIO/Hive). Full end-to-end
 * warehouse tests would live alongside and are best driven via the demo
 * script against the real stack.
 *
 * Runtime requirements:
 *   - Docker daemon reachable via DOCKER_HOST (or ~/.testcontainers.properties)
 *   - Daemon must accept API v1.41 or newer (Docker Engine 20.10+ /
 *     Rancher Desktop 1.9+ / Colima 0.5+ all satisfy this).
 *
 * Rancher Desktop tip: `export DOCKER_HOST=unix:///Users/$USER/.rd/docker.sock`
 * before `mvn verify`. If you still see "client version 1.32 is too old"
 * bump testcontainers to a version bundling docker-java 3.4+ (already
 * pinned in this pom).
 */
@Testcontainers
class PostgresJdbcIT {

    @Container
    static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("source").withUsername("sync").withPassword("sync");

    static SparkSession spark;

    @BeforeAll
    static void up() throws Exception {
        pg.start();
        try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
             java.sql.Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (id BIGSERIAL PRIMARY KEY, name TEXT, updated_at TIMESTAMPTZ DEFAULT NOW())");
            s.execute("INSERT INTO t (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')");
        }

        spark = SparkSession.builder()
                .appName("PostgresJdbcIT")
                .master("local[1]")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void down() {
        if (spark != null) spark.stop();
    }

    @Test
    void first_pull_returns_all_rows_and_advances_watermark() {
        SyncConfig cfg = buildConfig(WriteMode.APPEND_ONLY, "id", "long", 10);
        try (PostgresSource src = new PostgresSource(cfg)) {
            SourceBatch batch = src.readBatch(spark, cfg, Watermark.UNSET);
            assertThat(batch.rowCount()).isEqualTo(5);
            assertThat(batch.nextWatermark().type()).isEqualTo(Watermark.Type.LONG);
            assertThat(Long.parseLong(batch.nextWatermark().value())).isEqualTo(5L);
        }
    }

    @Test
    void incremental_pull_after_watermark_returns_only_new_rows() throws Exception {
        SyncConfig cfg = buildConfig(WriteMode.APPEND_ONLY, "id", "long", 10);
        // Read once to establish a watermark.
        Watermark w;
        try (PostgresSource src = new PostgresSource(cfg)) {
            w = src.readBatch(spark, cfg, Watermark.UNSET).nextWatermark();
        }
        // Insert two new rows.
        try (Connection c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
             java.sql.Statement s = c.createStatement()) {
            s.execute("INSERT INTO t (name) VALUES ('f'), ('g')");
        }
        // Second pull should return exactly those two.
        try (PostgresSource src = new PostgresSource(cfg)) {
            SourceBatch batch = src.readBatch(spark, cfg, w);
            assertThat(batch.rowCount()).isEqualTo(2);
            List<Row> rows = batch.rows().orderBy("id").collectAsList();
            assertThat((String) rows.get(0).getAs("name")).isEqualTo("f");
            assertThat((String) rows.get(1).getAs("name")).isEqualTo("g");
        }
    }

    @Test
    void batch_size_caps_returned_rows() {
        SyncConfig cfg = buildConfig(WriteMode.APPEND_ONLY, "id", "long", 2);
        try (PostgresSource src = new PostgresSource(cfg)) {
            SourceBatch batch = src.readBatch(spark, cfg, Watermark.UNSET);
            assertThat(batch.rowCount()).isEqualTo(2);
        }
    }

    private SyncConfig buildConfig(WriteMode mode, String incrCol, String incrType, int batchSize) {
        SyncConfig c = new SyncConfig();
        c.setName("t"); c.setSyncId("t_sync"); c.setVersion("1.0.0");

        SourceConfig s = new SourceConfig();
        s.setType("postgres");
        s.setSchema("public");
        s.setTable("t");
        s.setColumns(java.util.Arrays.asList("id", "name", "updated_at"));
        s.setPrimaryKey(Collections.singletonList("id"));
        s.setIncrementalColumn(incrCol);
        s.setIncrementalColumnType(incrType);

        Map<String, Object> conn = new HashMap<>();
        conn.put("host", pg.getHost());
        conn.put("port", pg.getMappedPort(5432).toString());
        conn.put("database", "source");
        conn.put("user", "sync");
        conn.put("password", "sync");
        s.setConnection(conn);
        c.setSource(s);

        c.setMode(mode);
        WarehouseConfig w = new WarehouseConfig();
        w.setTableName("warehouse.t");
        c.setWarehouse(w);

        ScheduleConfig sc = new ScheduleConfig();
        sc.setCadence("1m");
        sc.setBatchSize(batchSize);
        c.setSchedule(sc);
        return c;
    }
}
