package com.warehouse.sync.cli;

import com.warehouse.sync.spark.SparkSessionFactory;
import com.warehouse.sync.state.IcebergStateStore;
import com.warehouse.sync.state.RunRecord;
import com.warehouse.sync.state.StateStore;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "inspect",
        description = "Print watermark + recent run history for one or all syncs (from the Iceberg state store).")
public class InspectCommand implements Callable<Integer> {

    @Option(names = "--sync-id", description = "Restrict to one sync_id. Default: all.")
    String syncId;

    @Option(names = "--limit", defaultValue = "5", description = "Recent runs per sync (default: 5).")
    int limit;

    @Option(names = "--catalog", defaultValue = "iceberg")
    String catalog;

    @Override
    public Integer call() {
        SparkSession spark = SparkSessionFactory.build("warehouse-sync:inspect");
        try (IcebergStateStore state = new IcebergStateStore(spark, catalog)) {
            state.ensureTables();
            List<StateStore.InspectRow> rows = state.inspect(syncId, limit);
            if (rows.isEmpty()) {
                System.out.println("(no state — either no runs yet or wrong catalog=" + catalog + ")");
                return 0;
            }
            for (StateStore.InspectRow r : rows) {
                System.out.println();
                System.out.println("─── " + r.syncId + " ───");
                System.out.println("  watermark        : "
                        + (r.watermarkValue == null ? "UNSET" : r.watermarkValue + " (" + r.watermarkType + ")"));
                System.out.println("  updated_at       : " + r.watermarkUpdatedAt);
                System.out.println("  recent runs      : " + r.recentRuns.size());
                for (RunRecord rr : r.recentRuns) {
                    System.out.printf("    %s  %-6s  rows=%d  wm=%s→%s%s%n",
                            rr.startedAt,
                            rr.status,
                            rr.rowsSynced,
                            rr.watermarkBefore,
                            rr.watermarkAfter,
                            rr.errorMessage == null ? "" : ("  error=" + rr.errorMessage));
                }
            }
            return 0;
        } finally {
            spark.stop();
        }
    }
}
