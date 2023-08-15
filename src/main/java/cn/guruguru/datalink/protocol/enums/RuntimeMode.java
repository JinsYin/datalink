package cn.guruguru.datalink.protocol.enums;

/**
 * Runtime execution mode
 *
 * @see <a href="RuntimeExecutionMode">https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/common/RuntimeExecutionMode.html</a>
 */
public enum RuntimeMode {
    BATCH,
    STREAMING,
}