package com.warehouse.sync.envelope;

/**
 * Names of the metadata columns wrapped around every synced row. Kept in
 * one place so writers and downstream queries stay consistent.
 *
 * Mirrors structured-logger's envelope discipline (_log_type, _log_class,
 * _version, data) but flattened: the sync engine writes to typed Iceberg
 * columns, not a nested JSON blob, so metadata is added as extra columns
 * alongside the business fields.
 */
public final class SyncEnvelope {
    private SyncEnvelope() {}

    public static final String SYNC_ID          = "_sync_id";
    public static final String SYNC_VERSION     = "_sync_version";
    public static final String INGESTED_AT      = "_ingested_at";
    public static final String INGESTED_DATE    = "_ingested_date";
    public static final String SOURCE_OP        = "_source_op";     // UPSERT | SNAPSHOT | DELETE
    public static final String BATCH_ID         = "_batch_id";      // uuid per SyncEngine batch

    public static final String OP_UPSERT   = "UPSERT";
    public static final String OP_SNAPSHOT = "SNAPSHOT";
    public static final String OP_DELETE   = "DELETE";
}
