package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.interfaces.NodeDataSource;
import cn.guruguru.datalink.enums.DataSourceType;
import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MongoCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.KafkaScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.OracleScanNode;
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
@JsonSubTypes({
        // cdc
        @JsonSubTypes.Type(value = KafkaCdcNode.class, name = KafkaCdcNode.TYPE),
        @JsonSubTypes.Type(value = MysqlCdcNode.class, name = MysqlCdcNode.TYPE),
        @JsonSubTypes.Type(value = OracleCdcNode.class, name = OracleCdcNode.TYPE),
        @JsonSubTypes.Type(value = MongoCdcNode.class, name = MongoCdcNode.TYPE),
        // scan
        @JsonSubTypes.Type(value = OracleScanNode.class, name = OracleScanNode.TYPE),
        @JsonSubTypes.Type(value = MySqlScanNode.class, name = MySqlScanNode.TYPE),
        @JsonSubTypes.Type(value = KafkaScanNode.class, name = KafkaScanNode.TYPE),
})
@Data
@NoArgsConstructor
public abstract class ExtractNode implements Node, Serializable {

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

    @JsonCreator
    public ExtractNode(@JsonProperty("id") String id,
                       @JsonProperty("name") String name,
                       @JsonProperty("fields") List<DataField> fields,
                       @Nullable @JsonProperty("properties") Map<String, String> properties) {
        this.id = Preconditions.checkNotNull(id, "id is null");
        this.name = name;
        this.fields = Preconditions.checkNotNull(fields, "fields is null");
        Preconditions.checkState(!fields.isEmpty(), "fields is empty");
        this.properties = properties;
    }

    DataSourceType getDataSourceType() {
        return this.getClass().getAnnotation(NodeDataSource.class).value();
    }
}
