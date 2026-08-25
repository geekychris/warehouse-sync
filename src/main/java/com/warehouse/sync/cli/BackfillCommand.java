package com.warehouse.sync.cli;

import com.warehouse.sync.config.ConfigLoader;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.engine.SyncEngine;
import com.warehouse.sync.engine.SyncResult;
import com.warehouse.sync.spark.SparkSessionFactory;
import com.warehouse.sync.state.IcebergStateStore;
import com.warehouse.sync.state.Watermark;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "backfill",
        description = "Reset or move the watermark for one sync, then re-run. Use --watermark to set an exact value or --reset to start over from the beginning.")
public class BackfillCommand implements Callable<Integer> {

    @Option(names = {"-c", "--config"}, required = true) String configPath;
    @Option(names = "--watermark", description = "New watermark literal (e.g. '2026-01-01T00:00:00Z' or '0'). Mutually exclusive with --reset.")
    String watermark;
    @Option(names = "--reset", description = "Set the watermark to UNSET so the next run re-pulls the whole table.")
    boolean reset;
    @Option(names = "--no-run", description = "Only change the state store, don't trigger a sync.")
    boolean noRun;

    @Override
    public Integer call() {
        if ((watermark == null) == !reset) {
            System.err.println("Provide exactly one of --watermark <value> or --reset");
            return 2;
        }
        ConfigLoader loader = new ConfigLoader();
        List<SyncConfig> configs = loader.loadPath(configPath);
        if (configs.size() != 1) {
            System.err.println("backfill requires exactly one config; got " + configs.size());
            return 2;
        }
        SyncConfig cfg = configs.get(0);
        SparkSession spark = SparkSessionFactory.build("warehouse-sync:backfill");
        try (IcebergStateStore state = new IcebergStateStore(spark, cfg.getWarehouse().getTargetCatalog())) {
            state.ensureTables();
            Watermark w = reset
                    ? Watermark.UNSET
                    : Watermark.of(Watermark.parseType(cfg.getSource().getIncrementalColumnType()), watermark);
            state.setWatermark(cfg.getSyncId(), w, Instant.now());
            System.out.println("watermark for " + cfg.getSyncId() + " set to " + w);
            if (noRun) return 0;

            SyncEngine engine = new SyncEngine(spark, state);
            SyncResult r = engine.run(cfg);
            System.out.println(r);
            return "OK".equals(r.status) || "NOOP".equals(r.status) ? 0 : 1;
        } finally {
            spark.stop();
        }
    }
}
