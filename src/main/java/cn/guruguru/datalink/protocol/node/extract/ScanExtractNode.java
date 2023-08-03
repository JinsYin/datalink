package cn.guruguru.datalink.protocol.node.extract;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import cn.guruguru.datalink.protocol.node.extract.scan.JdbcScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.KafkaScanNode;
import cn.guruguru.datalink.protocol.node.extract.scan.MySqlScanNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Batch Extract Node
 *
 * @see org.apache.inlong.sort.protocol.node.ExtractNode
 * @see org.apache.inlong.sort.protocol.enums.ExtractMode
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = KafkaScanNode.class, name = "KafkaScan"),
        @JsonSubTypes.Type(value = JdbcScanNode.class, name = "JdbcScan"),
        @JsonSubTypes.Type(value = MySqlScanNode.class, name = "MysqlScan"),
})
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public abstract class ScanExtractNode extends ExtractNode {
    @JsonCreator
    public ScanExtractNode(@JsonProperty("id") String id,
                           @JsonProperty("name") String name,
                           @JsonProperty("fields") List<DataField> fields,
                           @Nullable @JsonProperty("properties") Map<String, String> properties) {
        super(id, name, fields, properties);
    }
}
