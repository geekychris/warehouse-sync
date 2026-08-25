package com.warehouse.sync.source.postgres;

import com.warehouse.sync.config.ConfigLoader;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.state.Watermark;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresSourceQueryTest {

    @Test
    void initial_pull_has_no_where_clause() throws Exception {
        SyncConfig cfg = loadCfg("append_only", "long");
        String sql = new PostgresSource(cfg).buildSubquery(cfg, Watermark.UNSET);
        assertThat(sql).contains("ORDER BY \"id\" ASC");
        assertThat(sql).contains("LIMIT 1000");
        assertThat(sql).doesNotContain("WHERE");
    }

    @Test
    void incremental_pull_includes_watermark_predicate() throws Exception {
        SyncConfig cfg = loadCfg("append_only", "long");
        String sql = new PostgresSource(cfg).buildSubquery(cfg,
                Watermark.of(Watermark.Type.LONG, "42"));
        assertThat(sql).contains("WHERE \"id\" > 42");
    }

    @Test
    void timestamp_lag_added_when_configured() throws Exception {
        SyncConfig cfg = loadCfg("merge_with_lag", "timestamp");
        String sql = new PostgresSource(cfg).buildSubquery(cfg,
                Watermark.of(Watermark.Type.TIMESTAMP, "2026-01-01T00:00:00Z"));
        assertThat(sql).contains("WHERE \"updated_at\" > TIMESTAMP '2026-01-01T00:00:00Z'");
        assertThat(sql).contains("AND \"updated_at\" < NOW() - INTERVAL '5 seconds'");
    }

    @Test
    void full_replace_has_no_where_no_limit() throws Exception {
        SyncConfig cfg = loadCfg("full_replace", "long");
        String sql = new PostgresSource(cfg).buildSubquery(cfg, Watermark.UNSET);
        assertThat(sql).doesNotContain("WHERE");
        assertThat(sql).doesNotContain("LIMIT");
        assertThat(sql).doesNotContain("ORDER BY");
    }

    private SyncConfig loadCfg(String flavor, String incrType) throws Exception {
        String body;
        switch (flavor) {
            case "append_only":
                body = "{"
                        + "\"name\":\"X\",\"sync_id\":\"x_sync\",\"version\":\"1.0.0\","
                        + "\"source\":{\"type\":\"postgres\",\"connection\":{\"host\":\"h\",\"database\":\"d\",\"user\":\"u\",\"password\":\"p\"},"
                        + "\"schema\":\"public\",\"table\":\"t\",\"columns\":[\"id\",\"name\"],"
                        + "\"incremental_column\":\"id\",\"incremental_column_type\":\"long\"},"
                        + "\"mode\":\"append_only\","
                        + "\"warehouse\":{\"table_name\":\"w.t\"},"
                        + "\"schedule\":{\"cadence\":\"1m\",\"batch_size\":1000}"
                        + "}";
                break;
            case "merge_with_lag":
                body = "{"
                        + "\"name\":\"X\",\"sync_id\":\"x_sync\",\"version\":\"1.0.0\","
                        + "\"source\":{\"type\":\"postgres\",\"connection\":{\"host\":\"h\",\"database\":\"d\",\"user\":\"u\",\"password\":\"p\"},"
                        + "\"schema\":\"public\",\"table\":\"t\",\"primary_key\":[\"id\"],"
                        + "\"incremental_column\":\"updated_at\",\"incremental_column_type\":\"timestamp\"},"
                        + "\"mode\":\"merge\","
                        + "\"warehouse\":{\"table_name\":\"w.t\"},"
                        + "\"schedule\":{\"cadence\":\"1m\",\"batch_size\":500,\"watermark_lag_seconds\":5}"
                        + "}";
                break;
            case "full_replace":
                body = "{"
                        + "\"name\":\"X\",\"sync_id\":\"x_sync\",\"version\":\"1.0.0\","
                        + "\"source\":{\"type\":\"postgres\",\"connection\":{\"host\":\"h\",\"database\":\"d\",\"user\":\"u\",\"password\":\"p\"},"
                        + "\"schema\":\"public\",\"table\":\"t\"},"
                        + "\"mode\":\"full_replace\","
                        + "\"warehouse\":{\"table_name\":\"w.t\"},"
                        + "\"schedule\":{\"cadence\":\"1h\",\"batch_size\":100000}"
                        + "}";
                break;
            default:
                throw new IllegalArgumentException(flavor);
        }
        Path p = Files.createTempFile(flavor, ".json");
        Files.writeString(p, body);
        return new ConfigLoader().load(p);
    }
}
