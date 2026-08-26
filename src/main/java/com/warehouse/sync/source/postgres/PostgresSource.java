package com.warehouse.sync.source.postgres;

import com.warehouse.sync.config.SourceConfig;
import com.warehouse.sync.config.SyncConfig;
import com.warehouse.sync.config.WriteMode;
import com.warehouse.sync.source.Source;
import com.warehouse.sync.source.SourceBatch;
import com.warehouse.sync.state.Watermark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Reads rows from Postgres via Spark's built-in JDBC datasource, using a
 * pushdown WHERE clause on the watermark column.
 *
 * Watermark advancement:
 *   - For append_only + merge: nextWatermark = max(incremental_column) over
 *     the returned batch (computed by Spark aggregation on the loaded rows).
 *   - For full_replace: no watermark used — engine calls with UNSET and
 *     ignores nextWatermark.
 */
public class PostgresSource implements Source {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSource.class);

    private final SyncConfig config;

    public PostgresSource(SyncConfig config) {
        this.config = config;
    }

    @Override
    public SourceBatch readBatch(SparkSession spark, SyncConfig cfg, Watermark previousWatermark) {
        SourceConfig src = cfg.getSource();
        String jdbcUrl = buildJdbcUrl(src.getConnection());
        Properties props = buildProps(src.getConnection());

        String subquery = buildSubquery(cfg, previousWatermark);
        LOG.info("Postgres pull: {}  (batch_size={})", subquery, cfg.getSchedule().getBatchSize());

        Dataset<Row> rows = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", "(" + subquery + ") AS t")
                .option("fetchsize", String.valueOf(cfg.getSchedule().getBatchSize()))
                .options(scala.collection.JavaConverters.mapAsScalaMap(propsToOptions(props)).toMap(
                        scala.Predef.$conforms()))
                .load()
                .cache();

        long count = rows.count();
        Watermark next;
        if (cfg.getMode() == WriteMode.FULL_REPLACE || src.getIncrementalColumn() == null) {
            next = Watermark.UNSET;
        } else if (count == 0) {
            next = previousWatermark;
        } else {
            String incrCol = src.getIncrementalColumn();
            // Spark SQL uses backticks for identifier quoting, not the double-quotes
            // that quoteIdent() emits for Postgres. Passing "updated_at" to selectExpr
            // is a string literal, not the column — max of a constant returns the
            // constant, and we'd persist "updated_at" as the watermark. Backticks.
            Row maxRow = rows.selectExpr("max(`" + incrCol.replace("`", "``") + "`) AS max_v").first();
            Object raw = maxRow.isNullAt(0) ? null : maxRow.get(0);
            next = raw == null ? previousWatermark
                    : Watermark.fromRaw(src.getIncrementalColumnType(), raw);
        }
        return new SourceBatch(rows, count, next);
    }

    /** Build the SELECT (...) subquery Spark uses via dbtable. */
    String buildSubquery(SyncConfig cfg, Watermark previousWatermark) {
        SourceConfig src = cfg.getSource();
        String cols = src.getColumns().isEmpty()
                ? "*"
                : String.join(", ", src.getColumns().stream().map(PostgresSource::quoteIdent).toArray(String[]::new));
        String tbl = src.getSchema() == null
                ? quoteIdent(src.getTable())
                : quoteIdent(src.getSchema()) + "." + quoteIdent(src.getTable());

        StringBuilder sb = new StringBuilder("SELECT ").append(cols).append(" FROM ").append(tbl);

        boolean incremental = cfg.getMode() != WriteMode.FULL_REPLACE
                && src.getIncrementalColumn() != null;

        if (incremental) {
            String incr = quoteIdent(src.getIncrementalColumn());
            if (!previousWatermark.isUnset()) {
                sb.append(" WHERE ").append(incr).append(" > ").append(previousWatermark.toSqlLiteral());
            }
            int lag = cfg.getSchedule().getWatermarkLagSeconds();
            if (lag > 0 && "timestamp".equalsIgnoreCase(src.getIncrementalColumnType())) {
                sb.append(previousWatermark.isUnset() ? " WHERE " : " AND ");
                sb.append(incr).append(" < NOW() - INTERVAL '").append(lag).append(" seconds'");
            }
            sb.append(" ORDER BY ").append(incr).append(" ASC");
            sb.append(" LIMIT ").append(cfg.getSchedule().getBatchSize());
        }
        return sb.toString();
    }

    static String buildJdbcUrl(Map<String, Object> connection) {
        String host = resolve(connection, "host_env", "host");
        String port = resolveOpt(connection, "port_env", "port", "5432");
        String db   = resolve(connection, "database_env", "database");
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + db;
        String sslmode = (String) connection.getOrDefault("sslmode", null);
        if (sslmode != null) url += "?sslmode=" + sslmode;
        return url;
    }

    static Properties buildProps(Map<String, Object> connection) {
        Properties p = new Properties();
        p.setProperty("user",     resolve(connection, "user_env", "user"));
        p.setProperty("password", resolve(connection, "password_env", "password"));
        p.setProperty("driver", "org.postgresql.Driver");
        return p;
    }

    private static Map<String, String> propsToOptions(Properties p) {
        java.util.HashMap<String, String> m = new java.util.HashMap<>();
        for (String n : p.stringPropertyNames()) m.put(n, p.getProperty(n));
        return m;
    }

    static String resolve(Map<String, Object> conn, String envKey, String directKey) {
        if (conn.containsKey(envKey)) {
            String envName = String.valueOf(conn.get(envKey));
            String v = System.getenv(envName);
            if (v == null || v.isEmpty()) {
                throw new IllegalStateException("Env var " + envName + " (from " + envKey + ") is unset");
            }
            return v;
        }
        if (conn.containsKey(directKey)) return String.valueOf(conn.get(directKey));
        throw new IllegalStateException("Neither " + envKey + " nor " + directKey + " set in connection");
    }

    static String resolveOpt(Map<String, Object> conn, String envKey, String directKey, String fallback) {
        try { return resolve(conn, envKey, directKey); }
        catch (IllegalStateException e) { return fallback; }
    }

    static String quoteIdent(String s) {
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }
}
