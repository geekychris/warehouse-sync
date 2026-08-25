package com.warehouse.sync.source.postgres;

import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.source.Source;
import com.warehouse.sync.source.SourceFactory;

import java.util.Arrays;
import java.util.List;

public class PostgresSourceFactory implements SourceFactory {

    private static final List<String> REQUIRED_CONNECTION_KEYS = Arrays.asList(
            "host_env|host",
            "database_env|database",
            "user_env|user",
            "password_env|password"
    );

    @Override
    public String type() { return "postgres"; }

    @Override
    public void validate(SyncConfig config) {
        if (config.getSource().getConnection() == null) {
            throw new IllegalArgumentException("source.connection is required for postgres");
        }
        for (String key : REQUIRED_CONNECTION_KEYS) {
            String[] alts = key.split("\\|");
            boolean present = false;
            for (String alt : alts) {
                if (config.getSource().getConnection().containsKey(alt)) { present = true; break; }
            }
            if (!present) {
                throw new IllegalArgumentException(
                        "source.connection is missing one of: " + Arrays.toString(alts) + " (postgres)");
            }
        }
        if (config.getSource().getTable() == null || config.getSource().getTable().isEmpty()) {
            throw new IllegalArgumentException("source.table is required for postgres");
        }
    }

    @Override
    public Source create(SyncConfig config) {
        return new PostgresSource(config);
    }
}
