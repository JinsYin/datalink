package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.JdbcScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.KafkaScanNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Data Node
 *
 * @see org.apache.inlong.sort.protocol.node.Node
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MysqlCdcNode.class, name = "mysql-cdc"),
        @JsonSubTypes.Type(value = JdbcScanNode.class, name = "jdbc-scan"),
        @JsonSubTypes.Type(value = KafkaScanNode.class, name = "kafka-scan"),
})
public interface Node {
    String getId();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    String getName();

    List<DataField> getFields();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    default Map<String, String> getProperties() {
        return new TreeMap<>();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    default Map<String, String> tableOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        if (getProperties() != null && !getProperties().isEmpty()) {
            options.putAll(getProperties());
        }
        return options;
    }

    String genTableName();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    default String getPrimaryKey() {
        return null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    default List<DataField> getPartitionFields() {
        return null;
    }
}
