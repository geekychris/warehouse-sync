package com.warehouse.sync.cli;

import com.warehouse.sync.config.ConfigLoader;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.source.Source;
import com.warehouse.sync.source.SourceBatch;
import com.warehouse.sync.source.SourceFactoryRegistry;
import com.warehouse.sync.spark.SparkSessionFactory;
import com.warehouse.sync.state.IcebergStateStore;
import com.warehouse.sync.state.Watermark;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "dry-run",
        description = "Runs the SELECT, prints row count + sample + next watermark. Does NOT write to warehouse and does NOT advance state.")
public class DryRunCommand implements Callable<Integer> {

    @Option(names = {"-c", "--config"}, required = true)
    String configPath;

    @Option(names = "--sample", defaultValue = "5", description = "Rows to print (default: 5).")
    int sampleRows;

    @Override
    public Integer call() {
        ConfigLoader loader = new ConfigLoader();
        List<SyncConfig> configs = loader.loadPath(configPath);
        SparkSession spark = SparkSessionFactory.build("warehouse-sync:dry-run");
        try (IcebergStateStore state = new IcebergStateStore(spark, configs.get(0).getWarehouse().getTargetCatalog())) {
            state.ensureTables();
            for (SyncConfig cfg : configs) {
                Watermark w = state.readWatermark(cfg.getSyncId(), cfg.getSource().getIncrementalColumnType());
                System.out.println();
                System.out.println("=== " + cfg.getSyncId() + " ===");
                System.out.println("  current watermark : " + w);
                Source src = SourceFactoryRegistry.instance().get(cfg.getSource().getType()).create(cfg);
                try (Source s = src) {
                    SourceBatch batch = s.readBatch(spark, cfg, w);
                    System.out.println("  rows returned     : " + batch.rowCount());
                    System.out.println("  next watermark    : " + batch.nextWatermark());
                    if (batch.rowCount() > 0) {
                        System.out.println("  sample:");
                        batch.rows().show(sampleRows, false);
                    }
                }
            }
            return 0;
        } finally {
            spark.stop();
        }
    }
}
