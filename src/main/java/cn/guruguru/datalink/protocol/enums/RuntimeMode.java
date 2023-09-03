package cn.guruguru.datalink.protocol.enums;

/**
 * Runtime execution mode
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/api/common/RuntimeExecutionMode.html">RuntimeExecutionMode</a>
 */
public enum RuntimeMode {
    /**
     * batch mode
     */
    BATCH,

    /**
     * streaming mode
     */
    STREAMING,
}