package cn.guruguru.datalink.protocol.enums;

/**
 * Runtime execution mode
 *
 * @see https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/common/RuntimeExecutionMode.html
 */
public enum RuntimeMode {
    // 离线同步，包括出/入湖，支持单表或多表
    BATCH,

    // 实时同步，目前仅限入湖，支持单表和整库
    STREAMING,
}