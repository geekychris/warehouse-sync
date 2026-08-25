package com.warehouse.sync.source.mysql;

import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.source.Source;
import com.warehouse.sync.source.SourceFactory;

/**
 * Stub proving the SPI is pluggable.
 *
 * validate() runs the same shape of connection-shape checks as postgres so
 * `validate-config` is useful. create() throws — real MySQL support is a
 * near-clone of {@link com.warehouse.sync.source.postgres.PostgresSource}
 * with a different JDBC URL and identifier quoting (`col` vs "col").
 */
public class MySqlSourceFactory implements SourceFactory {

    @Override
    public String type() { return "mysql"; }

    @Override
    public void validate(SyncConfig config) {
        if (config.getSource().getConnection() == null
                || config.getSource().getConnection().isEmpty()) {
            throw new IllegalArgumentException("source.connection is required for mysql");
        }
    }

    @Override
    public Source create(SyncConfig config) {
        throw new UnsupportedOperationException(
                "MySQL source is a stub in v0.1.0 — implement MySqlSource "
                        + "mirroring PostgresSource (JDBC URL: jdbc:mysql://..., identifier quote: `). "
                        + "The SPI already accepts it; only the reader is missing.");
    }
}
