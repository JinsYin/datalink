package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MongoCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.JdbcScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.KafkaScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.load.LakehouseLoadNode;
import cn.guruguru.datalink.protocol.node.transform.TransformNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

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
        // cdc
        @JsonSubTypes.Type(value = KafkaCdcNode.class, name = "KafkaCdc"),
        @JsonSubTypes.Type(value = MysqlCdcNode.class, name = "MysqlCdc"),
        @JsonSubTypes.Type(value = OracleCdcNode.class, name = "OracleCdc"),
        @JsonSubTypes.Type(value = MongoCdcNode.class, name = "MongoCdc"),
        // scan
        @JsonSubTypes.Type(value = JdbcScanNode.class, name = "JdbcScan"),
        @JsonSubTypes.Type(value = MySqlScanNode.class, name = "MysqlScan"),
        @JsonSubTypes.Type(value = KafkaScanNode.class, name = "KafkaScan"),
        // transform
        @JsonSubTypes.Type(value = TransformNode.class, name = "Transform"),
        // load
        @JsonSubTypes.Type(value = LakehouseLoadNode.class, name = "LakehouseLoad"),

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

    default String getNodeType() {
        return this.getClass().getAnnotation(JsonTypeName.class).value();
    }
}
