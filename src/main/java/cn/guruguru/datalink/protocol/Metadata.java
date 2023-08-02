package cn.guruguru.datalink.protocol;

import cn.guruguru.datalink.protocol.enums.MetaKey;

import java.util.Set;

/**
 * Metadata interface
 *
 * @see org.apache.inlong.sort.protocol.Metadata
 */
public interface Metadata {

    /**
     * Get the metadata key of MetaField that supported by Extract Nodes or Load Nodes
     *
     * @param metaKey The meta field
     * @return The key of metadata
     */
    default String getMetadataKey(MetaKey metaKey) {
        if (!supportedMetaFields().contains(metaKey)) {
            throw new UnsupportedOperationException(String.format("Unsupported meta field for %s: %s",
                    this.getClass().getSimpleName(), metaKey));
        }
        String metadataKey;
        switch (metaKey) {
            case TABLE_NAME:
                metadataKey = "table_name";
                break;
            case COLLECTION_NAME:
                metadataKey = "collection_name";
                break;
            case SCHEMA_NAME:
                metadataKey = "schema_name";
                break;
            case DATABASE_NAME:
                metadataKey = "database_name";
                break;
            case OP_TS:
                metadataKey = "op_ts";
                break;

            default:
                throw new UnsupportedOperationException(String.format("Unsupported meta field for %s: %s",
                        this.getClass().getSimpleName(), metaKey));
        }
        return metadataKey;
    }

    /**
     * Get metadata type MetaField that supported by Extract Nodes or Load Nodes
     *
     * @param metaKey The meta field
     * @return The type of metadata that based on Flink SQL types
     */
    default String getMetadataType(MetaKey metaKey) {
        if (!supportedMetaFields().contains(metaKey)) {
            throw new UnsupportedOperationException(String.format("Unsupported meta field for %s: %s",
                    this.getClass().getSimpleName(), metaKey));
        }
        String metadataType;
        switch (metaKey) {
            case TABLE_NAME:
            case DATABASE_NAME:
            case OP_TYPE:
            case DATA_CANAL:
            case DATA:
            case DATA_DEBEZIUM:
            case COLLECTION_NAME:
            case SCHEMA_NAME:
            case KEY:
            case VALUE:
            case HEADERS_TO_JSON_STR:
                metadataType = "STRING";
                break;
            case OP_TS:
            case TS:
            case TIMESTAMP:
                metadataType = "TIMESTAMP_LTZ(3)";
                break;
            case IS_DDL:
                metadataType = "BOOLEAN";
                break;
            case SQL_TYPE:
                metadataType = "MAP<STRING, INT>";
                break;
            case MYSQL_TYPE:
                metadataType = "MAP<STRING, STRING>";
                break;
            case ORACLE_TYPE:
                metadataType = "MAP<STRING, STRING>";
                break;
            case PK_NAMES:
                metadataType = "ARRAY<STRING>";
                break;
            case HEADERS:
                metadataType = "MAP<STRING, BINARY>";
                break;
            case BATCH_ID:
            case PARTITION:
            case OFFSET:
                metadataType = "BIGINT";
                break;
            case UPDATE_BEFORE:
                metadataType = "ARRAY<MAP<STRING, STRING>>";
                break;
            case DATA_BYTES:
            case DATA_BYTES_DEBEZIUM:
            case DATA_BYTES_CANAL:
                metadataType = "BYTES";
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupport meta field for %s: %s",
                        this.getClass().getSimpleName(), metaKey));
        }
        return metadataType;
    }

    /**
     * Is virtual.
     * By default, the planner assumes that a metadata column can be used for both reading and writing.
     * However, in many cases an external system provides more read-only metadata fields than writable fields.
     * Therefore, it is possible to exclude metadata columns from persisting using the VIRTUAL keyword.
     *
     * @param metaKey The meta field
     * @return true if it is virtual else false
     */
    boolean isVirtual(MetaKey metaKey);

    /**
     * Supported meta field set
     *
     * @return The set of supported meta field
     */
    Set<MetaKey> supportedMetaFields();

    /**
     * Format string by Flink SQL
     *
     * @param metaKey The meta field
     * @return The string that format by Flink SQL
     */
    default String format(MetaKey metaKey) {
        if (metaKey == MetaKey.PROCESS_TIME) {
            return "AS PROCTIME()";
        }
        return String.format("%s METADATA FROM '%s'%s",
                getMetadataType(metaKey), getMetadataKey(metaKey), isVirtual(metaKey) ? " VIRTUAL" : "");
    }
}
