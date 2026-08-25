package com.warehouse.sync.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigLoaderTest {

    @Test
    void loads_valid_merge_config() throws Exception {
        Path p = write("valid.json",
                "{"
                        + "\"name\":\"UsersSync\",\"sync_id\":\"users_sync\",\"version\":\"1.0.0\","
                        + "\"source\":{\"type\":\"postgres\",\"connection\":{\"host\":\"h\",\"database\":\"d\",\"user\":\"u\",\"password\":\"p\"},"
                        + "\"schema\":\"public\",\"table\":\"users\",\"primary_key\":[\"id\"],"
                        + "\"incremental_column\":\"updated_at\",\"incremental_column_type\":\"timestamp\"},"
                        + "\"mode\":\"merge\","
                        + "\"warehouse\":{\"table_name\":\"warehouse.users\"},"
                        + "\"schedule\":{\"cadence\":\"5m\",\"batch_size\":1000}"
                        + "}");
        SyncConfig c = new ConfigLoader().load(p);
        assertThat(c.getSyncId()).isEqualTo("users_sync");
        assertThat(c.getMode()).isEqualTo(WriteMode.MERGE);
        assertThat(c.getWarehouse().catalogQualifiedTable()).isEqualTo("iceberg.warehouse.users");
    }

    @Test
    void merge_without_primary_key_fails_structural() throws Exception {
        Path p = write("bad.json",
                "{"
                        + "\"name\":\"X\",\"sync_id\":\"x\",\"version\":\"1.0.0\","
                        + "\"source\":{\"type\":\"postgres\",\"connection\":{\"host\":\"h\",\"database\":\"d\",\"user\":\"u\",\"password\":\"p\"},"
                        + "\"table\":\"t\",\"incremental_column\":\"updated_at\",\"incremental_column_type\":\"timestamp\"},"
                        + "\"mode\":\"merge\","
                        + "\"warehouse\":{\"table_name\":\"w.t\"},"
                        + "\"schedule\":{\"cadence\":\"1m\",\"batch_size\":1}"
                        + "}");
        assertThatThrownBy(() -> new ConfigLoader().load(p))
                .isInstanceOf(ConfigValidationException.class)
                .hasMessageContaining("primary_key");
    }

    @Test
    void invalid_sync_id_pattern_fails_schema() throws Exception {
        Path p = write("bad_id.json",
                "{"
                        + "\"name\":\"X\",\"sync_id\":\"UPPERCASE\",\"version\":\"1.0.0\","
                        + "\"source\":{\"type\":\"postgres\",\"connection\":{},\"table\":\"t\"},"
                        + "\"mode\":\"full_replace\","
                        + "\"warehouse\":{\"table_name\":\"w.t\"},"
                        + "\"schedule\":{\"cadence\":\"1m\",\"batch_size\":1}"
                        + "}");
        assertThatThrownBy(() -> new ConfigLoader().load(p))
                .isInstanceOf(ConfigValidationException.class)
                .hasMessageContaining("sync_id");
    }

    @Test
    void ships_sample_configs_all_load() {
        // The three sample configs shipped in sync-configs/ must all validate.
        Path dir = Path.of("sync-configs");
        if (!Files.isDirectory(dir)) return;  // running outside the repo root
        assertThat(new ConfigLoader().loadDirectory(dir)).hasSize(3);
    }

    private Path write(String name, String contents) throws Exception {
        Path p = Files.createTempFile(name.replace(".json", "_"), ".json");
        Files.writeString(p, contents);
        return p;
    }
}
