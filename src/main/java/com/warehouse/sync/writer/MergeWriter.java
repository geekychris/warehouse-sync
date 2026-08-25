package com.warehouse.sync.writer;

import com.warehouse.sync.config.SyncConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Upsert-on-PK writer using Iceberg's MERGE INTO. The staged batch is
 * registered as a temp view; the merge matches on primary_key columns and
 * either UPDATE-SET or INSERT.
 *
 * DELETEs from the source are NOT captured — this mode assumes source
 * rows are either brand new or edited-in-place. If deletes matter, schedule
 * a full_refresh alongside this sync via {@code schedule.full_refresh_cadence}.
 */
public class MergeWriter implements WarehouseWriter {
    private static final Logger LOG = LoggerFactory.getLogger(MergeWriter.class);

    @Override
    public void ensureTable(SparkSession spark, SyncConfig config, Dataset<Row> sample) {
        WriterSupport.ensureTable(spark, config, sample);
    }

    @Override
    public void write(SparkSession spark, SyncConfig config, Dataset<Row> rows) {
        Dataset<Row> staged = WriterSupport.applyRenames(rows, config);
        String table = config.getWarehouse().catalogQualifiedTable();
        String viewName = "wh_sync_stage_" + config.getSyncId();
        staged.createOrReplaceTempView(viewName);

        List<String> pk = config.getSource().getPrimaryKey();
        String onClause = pk.stream()
                .map(c -> "t." + c + " = s." + c)
                .collect(Collectors.joining(" AND "));

        // Update every non-PK column on match; insert everything on miss.
        List<String> allCols = java.util.Arrays.stream(staged.schema().fieldNames()).collect(Collectors.toList());
        String setClause = allCols.stream()
                .filter(c -> !pk.contains(c))
                .map(c -> "t." + c + " = s." + c)
                .collect(Collectors.joining(", "));
        String insertCols = String.join(", ", allCols);
        String insertVals = allCols.stream().map(c -> "s." + c).collect(Collectors.joining(", "));

        String sql = "MERGE INTO " + table + " t USING " + viewName + " s ON " + onClause
                + " WHEN MATCHED THEN UPDATE SET " + setClause
                + " WHEN NOT MATCHED THEN INSERT (" + insertCols + ") VALUES (" + insertVals + ")";
        LOG.info("Merge write: {} rows → {} on PK ({})", staged.count(), table, String.join(",", pk));
        spark.sql(sql);
    }
}
