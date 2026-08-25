package com.warehouse.sync.cli;

import com.warehouse.sync.config.ConfigLoader;
import com.warehouse.sync.config.ConfigValidationException;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.source.SourceFactory;
import com.warehouse.sync.source.SourceFactoryRegistry;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;

@Command(
        name = "validate-config",
        description = "Validate a sync config file (or a directory of them) against the JSON schema, structural rules, and registered source types. Does NOT open network connections."
)
public class ValidateConfigCommand implements Callable<Integer> {

    @Option(names = {"-c", "--config"}, required = true,
            description = "Path to a JSON config file, or a directory containing *.json configs.")
    String configPath;

    @Override
    public Integer call() {
        ConfigLoader loader = new ConfigLoader();
        SourceFactoryRegistry registry = SourceFactoryRegistry.instance();

        List<SyncConfig> configs;
        try {
            configs = loader.loadPath(configPath);
        } catch (ConfigValidationException e) {
            System.err.println(e.getMessage());
            return 2;
        }

        int failed = 0;
        for (SyncConfig c : configs) {
            System.out.println("✓ " + c.getSyncId() + " (" + c.getName() + ") — schema + structural OK");

            // Check source type is a registered factory + let factory do its own preflight.
            String type = c.getSource().getType();
            try {
                SourceFactory factory = registry.get(type);
                factory.validate(c);
                System.out.println("  source.type '" + type + "' resolves to " + factory.getClass().getSimpleName());
            } catch (IllegalArgumentException e) {
                System.err.println("✗ " + c.getSyncId() + ": " + e.getMessage());
                failed++;
            }
        }

        System.out.println();
        System.out.println(configs.size() - failed + "/" + configs.size() + " configs valid.");
        return failed == 0 ? 0 : 1;
    }
}
