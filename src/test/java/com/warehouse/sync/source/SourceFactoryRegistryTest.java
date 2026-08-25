package com.warehouse.sync.source;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceFactoryRegistryTest {

    @Test
    void all_shipped_factories_are_discovered() {
        SourceFactoryRegistry r = SourceFactoryRegistry.instance();
        assertThat(r.all().keySet()).contains("postgres", "mysql", "debezium-kafka");
    }

    @Test
    void unknown_type_throws() {
        assertThatThrownBy(() -> SourceFactoryRegistry.instance().get("kafka-stream-nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown source type");
    }
}
