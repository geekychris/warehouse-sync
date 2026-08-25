package com.warehouse.sync.cli;

import picocli.CommandLine;

@CommandLine.Command(
        name = "warehouse-sync",
        mixinStandardHelpOptions = true,
        version = "warehouse-sync 0.1.0",
        description = "Metadata-driven incremental DB → Iceberg warehouse replication.",
        subcommands = {
                ValidateConfigCommand.class,
                RunCommand.class,
                DryRunCommand.class,
                InspectCommand.class,
                BackfillCommand.class,
                ServeCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class Main implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int code = new CommandLine(new Main()).execute(args);
        System.exit(code);
    }
}
