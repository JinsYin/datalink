package cn.guruguru.datalink.protocol.node;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MongoCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import cn.guruguru.datalink.protocol.node.extract.scan.JdbcScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.KafkaScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
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
        @JsonSubTypes.Type(value = KafkaCdcNode.class, name = "KafkaCdc"),
        @JsonSubTypes.Type(value = MysqlCdcNode.class, name = "MysqlCdc"),
        @JsonSubTypes.Type(value = OracleCdcNode.class, name = "OracleCdc"),
        @JsonSubTypes.Type(value = MongoCdcNode.class, name = "MongoCdc"),
        // scan
        @JsonSubTypes.Type(value = JdbcScanNode.class, name = "JdbcScan"),
        @JsonSubTypes.Type(value = MySqlScanNode.class, name = "MysqlScan"),
        @JsonSubTypes.Type(value = KafkaScanNode.class, name = "KafkaScan"),
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
}
