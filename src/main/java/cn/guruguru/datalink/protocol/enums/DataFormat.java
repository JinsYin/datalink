package cn.guruguru.datalink.protocol.enums;

/**
 * Table formats supported Flink
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/overview/">Flink formats</a>
 */
public enum DataFormat {
    JSON("json"),
    CSV("csv"),
    AVRO("avro"),
    AVRO_CONFLUENT("avro-confluent"),
    DEBEZIUM_JSON("debezium-json"),
    CANAL_JSON("canal-json"),
    MAXWELL_JSON("maxwell-json"),
    OGG_JSON("ogg-json"),
    PARQUET("parquet"),
    ORC("orc"),
    RAW("raw"),
    ;

    private final String format;

    DataFormat(String format) {
        this.format = format;
    }

    public String getFormat() {
        return this.format;
    }
}
