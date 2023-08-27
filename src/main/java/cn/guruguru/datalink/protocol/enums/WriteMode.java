package cn.guruguru.datalink.protocol.enums;

/**
 * Write mode
 *
 * @see <a href="https://arctic.netease.com/ch/flink/flink-dml/#writing-with-sql">Arctic Writing With SQL</a>
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/insert/">Flink INSERT Statement</a>
 */
public enum WriteMode {
    OVERWRITE,
    APPEND,
    UPSERT
}
