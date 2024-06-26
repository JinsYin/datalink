package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.datasource.NodeDataSource;
import cn.guruguru.datalink.datasource.DataSourceType;
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
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Data node for extracting.
 *
 * @see org.apache.inlong.sort.protocol.node.ExtractNode
 * @see org.apache.inlong.sort.protocol.enums.ExtractMode
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ // When using the ExtractNode as a generic type for deserialization
        // cdc
        @JsonSubTypes.Type(value = KafkaNode.class, name = KafkaNode.TYPE),
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
})
@Data
@NoArgsConstructor
public abstract class ExtractNode implements Node, Serializable {
    private static final long serialVersionUID = 5101982774852677003L;

    @JsonProperty("id")
    private String id;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("name")
    private String name;
    @JsonProperty("fields")
    private List<DataField> fields;
    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("properties")
    private Map<String, String> properties;
    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("propDescriptor")
    private NodePropDescriptor propDescriptor;

    @JsonCreator
    public ExtractNode(@JsonProperty("id") String id,
                       @JsonProperty("name") String name,
                       @JsonProperty("fields") List<DataField> fields,
                       @Nullable @JsonProperty("properties") Map<String, String> properties,
                       @Nullable @JsonProperty("propDescriptor") NodePropDescriptor propDescriptor) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = name;
        this.fields = Preconditions.checkNotNull(fields, "fields is null");
        Preconditions.checkState(!fields.isEmpty(), "fields is empty");
        this.properties = properties;
        this.propDescriptor = propDescriptor;
    }

    @Override
    public NodePropDescriptor getPropDescriptor(EngineType engineType) {
        if (propDescriptor == null) {
            return Node.super.getPropDescriptor(engineType);
        }
        return propDescriptor;
    }

    DataSourceType getDataSourceType() {
        return this.getClass().getAnnotation(NodeDataSource.class).value();
    }
}
