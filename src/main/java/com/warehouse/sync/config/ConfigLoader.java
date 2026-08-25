package com.warehouse.sync.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Load and validate SyncConfig JSON files.
 *
 * Two-phase validation:
 *   1. JSON Schema (types, enums, required fields, regex patterns)
 *   2. {@link SyncConfig#validateStructural()} — mode-specific rules
 *      (merge needs PK, append_only needs watermark column, etc.)
 *
 * If either phase fails, {@link ConfigValidationException} is thrown with
 * a human-readable multi-line message. Callers surface it to the CLI.
 */
public class ConfigLoader {

    private static final String SCHEMA_RESOURCE = "sync-config-schema.json";

    private final ObjectMapper mapper;
    private final JsonSchema schema;

    public ConfigLoader() {
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(SCHEMA_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource: " + SCHEMA_RESOURCE);
            }
            JsonNode schemaNode = mapper.readTree(in);
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            this.schema = factory.getSchema(schemaNode);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load sync-config-schema.json", e);
        }
    }

    /** Load a single config file. */
    public SyncConfig load(Path file) {
        JsonNode node;
        try {
            node = mapper.readTree(file.toFile());
        } catch (IOException e) {
            throw new ConfigValidationException(file.toString(),
                    Collections.singletonList("Failed to parse JSON: " + e.getMessage()));
        }

        Set<ValidationMessage> schemaErrors = schema.validate(node);
        if (!schemaErrors.isEmpty()) {
            List<String> msgs = schemaErrors.stream()
                    .map(ValidationMessage::getMessage)
                    .sorted()
                    .collect(Collectors.toList());
            throw new ConfigValidationException(file.toString(), msgs);
        }

        SyncConfig config;
        try {
            config = mapper.treeToValue(node, SyncConfig.class);
        } catch (Exception e) {
            throw new ConfigValidationException(file.toString(),
                    Collections.singletonList("Deserialization failed: " + e.getMessage()));
        }

        try {
            config.validateStructural();
        } catch (IllegalArgumentException e) {
            throw new ConfigValidationException(file.toString(),
                    Collections.singletonList(e.getMessage()));
        }
        return config;
    }

    /** Load all *.json files in a directory (non-recursive). */
    public List<SyncConfig> loadDirectory(Path dir) {
        List<SyncConfig> out = new ArrayList<>();
        try (Stream<Path> paths = Files.list(dir)) {
            List<Path> jsonFiles = paths
                    .filter(p -> p.toString().endsWith(".json"))
                    .sorted()
                    .collect(Collectors.toList());
            for (Path p : jsonFiles) {
                out.add(load(p));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to list configs in " + dir, e);
        }
        return out;
    }

    /** Convenience: accept either a file or a directory. */
    public List<SyncConfig> loadPath(String path) {
        Path p = Paths.get(path);
        if (Files.isDirectory(p)) return loadDirectory(p);
        return Collections.singletonList(load(p));
    }
}
