package cn.guruguru.datalink.protocol.node.extract;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import cn.guruguru.datalink.protocol.field.WatermarkField;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.KafkaNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MongoCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.MysqlCdcNode;
import cn.guruguru.datalink.protocol.node.extract.cdc.OracleCdcNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Streaming Extract Node
 *
 * @see org.apache.inlong.sort.protocol.node.ExtractNode
 * @see org.apache.inlong.sort.protocol.enums.ExtractMode
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = KafkaNode.class, name = KafkaNode.TYPE),
        @JsonSubTypes.Type(value = KafkaCdcNode.class, name = KafkaCdcNode.TYPE),
        @JsonSubTypes.Type(value = MysqlCdcNode.class, name = MysqlCdcNode.TYPE),
        @JsonSubTypes.Type(value = OracleCdcNode.class, name = OracleCdcNode.TYPE),
        @JsonSubTypes.Type(value = MongoCdcNode.class, name = MongoCdcNode.TYPE),
})
@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public abstract class CdcExtractNode extends ExtractNode {
    private static final long serialVersionUID = 1143686897523799048L;

    @Nullable
    @JsonProperty("watermarkField")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private WatermarkField watermarkField;

    @JsonCreator
    public CdcExtractNode(@JsonProperty("id") String id,
                          @JsonProperty("name") String name,
                          @JsonProperty("fields") List<DataField> fields,
                          @Nullable @JsonProperty("properties") Map<String, String> properties,
                          @Nullable @JsonProperty("watermarkField") WatermarkField watermarkField) {
        super(id, name, fields, properties);
        this.watermarkField = watermarkField;
    }
}
