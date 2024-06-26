package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.parser.EngineType;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MongoCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.DmScanNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaNode;
import cn.guruguru.datalink.protocol.node.extract.scan.GreenplumScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.PostgresqlScanNode;
import cn.guruguru.datalink.protocol.node.load.AmoroLoadNode;
import cn.guruguru.datalink.protocol.node.transform.TransformNode;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

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
        @JsonSubTypes.Type(value = KafkaNode.class, name = KafkaNode.TYPE), // "type"=KafkaNode.TYPE -> KafkaNode.class
        @JsonSubTypes.Type(value = KafkaCdcNode.class, name = KafkaCdcNode.TYPE),
        @JsonSubTypes.Type(value = MysqlCdcNode.class, name = MysqlCdcNode.TYPE),
        @JsonSubTypes.Type(value = OracleCdcNode.class, name = OracleCdcNode.TYPE),
        @JsonSubTypes.Type(value = MongoCdcNode.class, name = MongoCdcNode.TYPE),
        // scan
        @JsonSubTypes.Type(value = MySqlScanNode.class, name = MySqlScanNode.TYPE),
        @JsonSubTypes.Type(value = OracleScanNode.class, name = OracleScanNode.TYPE),
        @JsonSubTypes.Type(value = DmScanNode.class, name = DmScanNode.TYPE),
        @JsonSubTypes.Type(value = PostgresqlScanNode.class, name = PostgresqlScanNode.TYPE),
        @JsonSubTypes.Type(value = GreenplumScanNode.class, name = GreenplumScanNode.TYPE),
        // transform
        @JsonSubTypes.Type(value = TransformNode.class, name = TransformNode.TYPE),
        // load
        @JsonSubTypes.Type(value = AmoroLoadNode.class, name = AmoroLoadNode.TYPE),

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
    default NodePropDescriptor getPropDescriptor(EngineType engineType) {
        switch (engineType) {
            case FLINK_SQL:
                return NodePropDescriptor.WITH;
            case SPARK_SQL:
                return NodePropDescriptor.OPTIONS; // The default is `OPTIONS`, it can also be set to `TBLPROPERTIES`
            default:
                return null;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    default Map<String, String> tableOptions(EngineType engineType) {
        Map<String, String> options = new LinkedHashMap<>();
        if (getProperties() != null && !getProperties().isEmpty()) {
            options.putAll(getProperties());
        }
        return options;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    String genTableName();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    default String getPrimaryKey() {
        return null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    default List<DataField> getPartitionFields() {
        return null;
    }

    @JsonIgnore
    default String getNodeType() {
        return this.getClass().getAnnotation(JsonTypeName.class).value();
    }

    @JsonIgnore
    default String quoteIdentifier(String identifier) {
        // if it is a schema object
        //if (identifier.contains(".")) {
        //    return "`" + identifier.replaceAll("\\.", "`.`") + "`";
        //}
        return "`" + identifier + "`";
    }

    static Node deserialize(String json) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, Node.class);
    }

    static String serialize(Node node) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(node);
    }
}
