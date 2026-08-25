package com.warehouse.sync.state;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WatermarkTest {

    @Test
    void long_from_raw_and_compare() {
        Watermark a = Watermark.fromRaw("long", 100L);
        Watermark b = Watermark.fromRaw("long", 500L);
        assertThat(a.compareTo(b)).isNegative();
        assertThat(b.compareTo(a)).isPositive();
        assertThat(a.compareTo(a)).isZero();
    }

    @Test
    void timestamp_from_raw_uses_iso_string() {
        Instant now = Instant.parse("2026-01-15T10:00:00Z");
        Watermark w = Watermark.fromRaw("timestamp", Timestamp.from(now));
        assertThat(w.value()).isEqualTo("2026-01-15T10:00:00Z");
        assertThat(w.toSqlLiteral()).isEqualTo("TIMESTAMP '2026-01-15T10:00:00Z'");
    }

    @Test
    void unset_is_less_than_any_value() {
        Watermark set = Watermark.of(Watermark.Type.LONG, "0");
        assertThat(Watermark.UNSET.compareTo(set)).isNegative();
        assertThat(set.compareTo(Watermark.UNSET)).isPositive();
        assertThat(Watermark.UNSET.compareTo(Watermark.UNSET)).isZero();
    }

    @Test
    void cross_type_compare_fails_fast() {
        Watermark a = Watermark.of(Watermark.Type.LONG, "1");
        Watermark b = Watermark.of(Watermark.Type.STRING, "1");
        assertThatThrownBy(() -> a.compareTo(b))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sql_literal_escapes_string_quotes() {
        Watermark w = Watermark.of(Watermark.Type.STRING, "O'Neill");
        assertThat(w.toSqlLiteral()).isEqualTo("'O''Neill'");
    }
}
