package com.warehouse.sync.source;

import com.warehouse.sync.config.SyncConfig;

/**
 * ServiceLoader-discovered factory for {@link Source} implementations.
 * <p>
 * To add a new source type:
 *   1. Implement {@link Source} and this factory.
 *   2. Add the factory class to
 *      {@code META-INF/services/com.warehouse.sync.source.SourceFactory}.
 *   3. Reference it in a config via {@code source.type = "<yourType>"}.
 */
public interface SourceFactory {
    /** Discriminator matched against {@code source.type} in the config. */
    String type();

    /**
     * Fail fast if this factory cannot handle the given config
     * (missing connection params, unsupported column type, etc.).
     * Called during validate-config; must not open network connections.
     */
    void validate(SyncConfig config);

    /** Construct a Source for a single run. */
    Source create(SyncConfig config);
}
