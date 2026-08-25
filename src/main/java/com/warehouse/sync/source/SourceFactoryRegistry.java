package com.warehouse.sync.source;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Registry of {@link SourceFactory} instances discovered via ServiceLoader.
 *
 * Thread-safe read-only after construction. Instantiated lazily on first
 * call to {@link #instance()}.
 */
public final class SourceFactoryRegistry {

    private static volatile SourceFactoryRegistry INSTANCE;

    private final Map<String, SourceFactory> byType;

    private SourceFactoryRegistry() {
        Map<String, SourceFactory> m = new LinkedHashMap<>();
        for (SourceFactory f : ServiceLoader.load(SourceFactory.class)) {
            SourceFactory prev = m.putIfAbsent(f.type(), f);
            if (prev != null) {
                throw new IllegalStateException(
                        "Duplicate SourceFactory for type '" + f.type() + "': "
                                + prev.getClass() + " vs " + f.getClass());
            }
        }
        this.byType = m;
    }

    public static SourceFactoryRegistry instance() {
        SourceFactoryRegistry r = INSTANCE;
        if (r == null) {
            synchronized (SourceFactoryRegistry.class) {
                if (INSTANCE == null) INSTANCE = new SourceFactoryRegistry();
                r = INSTANCE;
            }
        }
        return r;
    }

    public SourceFactory get(String type) {
        SourceFactory f = byType.get(type);
        if (f == null) {
            throw new IllegalArgumentException("Unknown source type '" + type
                    + "'. Registered types: " + byType.keySet());
        }
        return f;
    }

    public Map<String, SourceFactory> all() { return byType; }
}
