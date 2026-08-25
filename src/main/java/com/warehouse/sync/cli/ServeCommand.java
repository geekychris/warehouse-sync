package com.warehouse.sync.cli;

import com.warehouse.sync.config.ConfigLoader;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.engine.SyncEngine;
import com.warehouse.sync.scheduler.DaemonScheduler;
import com.warehouse.sync.spark.SparkSessionFactory;
import com.warehouse.sync.state.IcebergStateStore;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "serve",
        description = "Long-running daemon. Reads all configs in --config-dir and runs each on its declared schedule.cadence. Ctrl-C for graceful shutdown.")
public class ServeCommand implements Callable<Integer> {

    @Option(names = {"-d", "--config-dir"}, required = true,
            description = "Directory of *.json config files to schedule.")
    String configDir;

    @Override
    public Integer call() throws InterruptedException {
        ConfigLoader loader = new ConfigLoader();
        List<SyncConfig> configs = loader.loadPath(configDir);
        if (configs.isEmpty()) {
            System.err.println("No configs found in " + configDir);
            return 2;
        }

        SparkSession spark = SparkSessionFactory.build("warehouse-sync:serve");
        try (IcebergStateStore state = new IcebergStateStore(spark, configs.get(0).getWarehouse().getTargetCatalog())) {
            SyncEngine engine = new SyncEngine(spark, state);
            DaemonScheduler daemon = new DaemonScheduler(engine, configs);
            daemon.start();
            daemon.awaitShutdown();
            return 0;
        } finally {
            spark.stop();
        }
    }
}
