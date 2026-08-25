package com.warehouse.sync.state;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Typed watermark value — the last incremental_column value the engine has
 * successfully persisted for a given sync_id.
 *
 * Storage in the state table is (value_string, value_type). Values are
 * serialized as strings so all supported types share one column layout;
 * the {@code type} tag drives interpretation at read time.
 *
 * {@link #UNSET} means "never synced" — engine treats it as "-infinity"
 * so the first run pulls the whole table.
 */
public final class Watermark {

    public enum Type { TIMESTAMP, LONG, INT, DATE, STRING, UNSET }

    public static final Watermark UNSET = new Watermark(Type.UNSET, null);

    private final Type type;
    private final String value;   // canonical string form; null iff UNSET

    private Watermark(Type type, String value) {
        this.type = type;
        this.value = value;
    }

    public Type type() { return type; }
    public String value() { return value; }
    public boolean isUnset() { return type == Type.UNSET; }

    public static Watermark of(Type type, String value) {
        if (type == Type.UNSET) return UNSET;
        return new Watermark(type, Objects.requireNonNull(value, "value"));
    }

    /** Build a Watermark of the given wire-type from a raw Java value. */
    public static Watermark fromRaw(String wireType, Object raw) {
        if (raw == null) return UNSET;
        Type t = parseType(wireType);
        String s;
        switch (t) {
            case TIMESTAMP:
                if (raw instanceof Timestamp) s = ((Timestamp) raw).toInstant().toString();
                else if (raw instanceof Instant) s = raw.toString();
                else s = raw.toString();
                break;
            case LONG:
                s = String.valueOf(((Number) raw).longValue());
                break;
            case INT:
                s = String.valueOf(((Number) raw).intValue());
                break;
            case DATE:
                if (raw instanceof java.sql.Date) s = ((java.sql.Date) raw).toLocalDate().toString();
                else if (raw instanceof LocalDate) s = raw.toString();
                else s = raw.toString();
                break;
            case STRING:
                s = raw.toString();
                break;
            default:
                throw new IllegalStateException("unreachable: " + t);
        }
        return of(t, s);
    }

    public static Type parseType(String wireType) {
        if (wireType == null) throw new IllegalArgumentException("incremental_column_type is required");
        switch (wireType.toLowerCase()) {
            case "timestamp": return Type.TIMESTAMP;
            case "long":      return Type.LONG;
            case "int":       return Type.INT;
            case "date":      return Type.DATE;
            case "string":    return Type.STRING;
            default: throw new IllegalArgumentException("Unsupported incremental_column_type: " + wireType);
        }
    }

    /** SQL literal safe for inclusion in a WHERE clause. */
    public String toSqlLiteral() {
        if (isUnset()) throw new IllegalStateException("cannot render UNSET watermark as SQL literal");
        switch (type) {
            case TIMESTAMP: return "TIMESTAMP '" + value + "'";
            case DATE:      return "DATE '" + value + "'";
            case LONG:
            case INT:       return value;
            case STRING:    return "'" + value.replace("'", "''") + "'";
            default: throw new IllegalStateException("unreachable");
        }
    }

    /**
     * Compare two watermarks of the same type. Returns negative / zero /
     * positive per {@link Comparable#compareTo}. Throws if types differ.
     * UNSET compares as -infinity (less than any concrete value).
     */
    public int compareTo(Watermark other) {
        if (this.isUnset() && other.isUnset()) return 0;
        if (this.isUnset()) return -1;
        if (other.isUnset()) return 1;
        if (this.type != other.type) {
            throw new IllegalArgumentException("Type mismatch: " + this.type + " vs " + other.type);
        }
        switch (type) {
            case LONG:      return Long.compare(Long.parseLong(value), Long.parseLong(other.value));
            case INT:       return Integer.compare(Integer.parseInt(value), Integer.parseInt(other.value));
            case TIMESTAMP: return Instant.parse(value).compareTo(Instant.parse(other.value));
            case DATE:      return LocalDate.parse(value).compareTo(LocalDate.parse(other.value));
            case STRING:    return value.compareTo(other.value);
            default: throw new IllegalStateException("unreachable");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Watermark)) return false;
        Watermark w = (Watermark) o;
        return type == w.type && Objects.equals(value, w.value);
    }

    @Override
    public int hashCode() { return Objects.hash(type, value); }

    @Override
    public String toString() {
        return isUnset() ? "Watermark[UNSET]" : "Watermark[" + type + "=" + value + "]";
    }
}
