package com.warehouse.sync.config;

import java.util.List;

/**
 * Thrown by {@link ConfigLoader} when a config file fails validation.
 * Preserves the file path and list of individual error messages so the
 * CLI can render them cleanly.
 */
public class ConfigValidationException extends RuntimeException {
    private final String file;
    private final List<String> errors;

    public ConfigValidationException(String file, List<String> errors) {
        super(buildMessage(file, errors));
        this.file = file;
        this.errors = errors;
    }

    public String getFile() { return file; }
    public List<String> getErrors() { return errors; }

    private static String buildMessage(String file, List<String> errors) {
        StringBuilder sb = new StringBuilder("Invalid config: ").append(file);
        for (String e : errors) {
            sb.append("\n  - ").append(e);
        }
        return sb.toString();
    }
}
