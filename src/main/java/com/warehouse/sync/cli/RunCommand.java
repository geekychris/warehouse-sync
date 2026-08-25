package com.warehouse.sync.cli;

import com.warehouse.sync.config.ConfigLoader;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.engine.SyncEngine;
import com.warehouse.sync.engine.SyncResult;
import com.warehouse.sync.spark.SparkSessionFactory;
import com.warehouse.sync.state.IcebergStateStore;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "run",
        description = "Run one sync end-to-end and exit. --config points at either a JSON file or a directory of configs.")
public class RunCommand implements Callable<Integer> {

    @Option(names = {"-c", "--config"}, required = true,
            description = "Path to a config file OR directory of configs.")
    String configPath;

    @Option(names = "--force-full-refresh",
            description = "Ignore the config's declared mode and do a full-replace this run.")
    boolean forceFull;

    @Override
    public Integer call() {
        ConfigLoader loader = new ConfigLoader();
        List<SyncConfig> configs = loader.loadPath(configPath);

        SparkSession spark = SparkSessionFactory.build("warehouse-sync:run");
        try (IcebergStateStore state = new IcebergStateStore(spark, configs.get(0).getWarehouse().getTargetCatalog())) {
            SyncEngine engine = new SyncEngine(spark, state);
            int failed = 0;
            for (SyncConfig cfg : configs) {
                SyncResult r = forceFull ? engine.runFullRefresh(cfg) : engine.run(cfg);
                System.out.println(r);
                if ("FAILED".equals(r.status)) failed++;
            }
            return failed == 0 ? 0 : 1;
        } finally {
            spark.stop();
        }
    }
}
