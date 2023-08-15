package cn.guruguru.datalink.protocol.enums;

/**
 * Write mode
 *
 * @see <a href="Arctic Writing With SQL">https://arctic.netease.com/ch/flink/flink-dml/#writing-with-sql</a>
 * @see <a href="Flink INSERT Statement">https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/insert/</a>
 */
public enum WriteMode {
    OVERWRITE,
    APPEND,
    UPSERT
}
