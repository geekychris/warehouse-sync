package com.warehouse.sync.source.debezium;

import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.source.Source;
import com.warehouse.sync.source.SourceFactory;

/**
 * Stub proving the CDC-style seam fits the same Source SPI.
 *
 * The intended wiring: Debezium (running elsewhere, e.g. Kafka Connect)
 * writes one Kafka topic per source table with {@code op}, {@code before},
 * {@code after}, and {@code source.lsn} in each message. A real
 * DebeziumKafkaSource would:
 *
 *   1. Consume from that topic with a stable consumer-group id derived
 *      from sync_id, using {@code source.type = "debezium-kafka"} and
 *      {@code source.connection.topic = "..."} in the config.
 *   2. Filter to op ∈ {c, u, r} for merge mode; op ∈ {c} for append_only.
 *   3. Return a Dataset<Row> of the {@code after} payload, with
 *      nextWatermark = max(source.lsn) — the LSN is the incremental_column.
 *   4. Kafka commit offsets AFTER the writer commits, mirroring the
 *      watermark-advance semantics of PostgresSource.
 *
 * The config would look like:
 *   "source": {
 *     "type": "debezium-kafka",
 *     "connection": {
 *       "bootstrap_servers_env": "KAFKA_BOOTSTRAP",
 *       "topic": "cdc.public.users"
 *     },
 *     "primary_key": ["id"],
 *     "incremental_column": "source.lsn",
 *     "incremental_column_type": "long"
 *   }
 */
public class DebeziumKafkaSourceFactory implements SourceFactory {

    @Override
    public String type() { return "debezium-kafka"; }

    @Override
    public void validate(SyncConfig config) {
        Object topic = config.getSource().getConnection().get("topic");
        if (topic == null) {
            throw new IllegalArgumentException(
                    "source.connection.topic is required for debezium-kafka");
        }
    }

    @Override
    public Source create(SyncConfig config) {
        throw new UnsupportedOperationException(
                "DebeziumKafkaSource is a stub in v0.1.0 — see class javadoc for the wiring recipe. "
                        + "The SPI accepts it; only the Kafka consumer + Debezium-envelope parsing is missing.");
    }
}
