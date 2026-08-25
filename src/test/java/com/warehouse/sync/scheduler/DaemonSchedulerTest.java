package com.warehouse.sync.scheduler;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class DaemonSchedulerTest {

    @Test
    void cron_translations() {
        assertThat(DaemonScheduler.toCron(Duration.ofMinutes(1))).isEqualTo("*/1 * * * *");
        assertThat(DaemonScheduler.toCron(Duration.ofMinutes(5))).isEqualTo("*/5 * * * *");
        assertThat(DaemonScheduler.toCron(Duration.ofHours(2))).isEqualTo("0 */2 * * *");
        assertThat(DaemonScheduler.toCron(Duration.ofDays(1))).isEqualTo("0 0 */1 * *");
    }
}
