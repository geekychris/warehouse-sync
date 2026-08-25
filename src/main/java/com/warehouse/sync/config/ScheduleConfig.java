package com.warehouse.sync.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ScheduleConfig {
    private static final Pattern DUR = Pattern.compile("^([0-9]+)(s|m|h|d)$");

    private String cadence;
    @JsonProperty("batch_size")
    private int batchSize;
    @JsonProperty("full_refresh_cadence")
    private String fullRefreshCadence;
    @JsonProperty("watermark_lag_seconds")
    private int watermarkLagSeconds = 0;

    public String getCadence() { return cadence; }
    public void setCadence(String cadence) { this.cadence = cadence; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public String getFullRefreshCadence() { return fullRefreshCadence; }
    public void setFullRefreshCadence(String fullRefreshCadence) { this.fullRefreshCadence = fullRefreshCadence; }

    public int getWatermarkLagSeconds() { return watermarkLagSeconds; }
    public void setWatermarkLagSeconds(int watermarkLagSeconds) { this.watermarkLagSeconds = watermarkLagSeconds; }

    public Duration cadenceDuration() { return parseDuration(cadence); }
    public Duration fullRefreshCadenceDuration() {
        return fullRefreshCadence == null ? null : parseDuration(fullRefreshCadence);
    }

    public static Duration parseDuration(String s) {
        Matcher m = DUR.matcher(s);
        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid duration string: " + s + " (expected e.g. '30s', '5m', '1h')");
        }
        long v = Long.parseLong(m.group(1));
        switch (m.group(2)) {
            case "s": return Duration.ofSeconds(v);
            case "m": return Duration.ofMinutes(v);
            case "h": return Duration.ofHours(v);
            case "d": return Duration.ofDays(v);
            default: throw new IllegalStateException("unreachable");
        }
    }
}
