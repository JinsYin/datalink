package cn.guruguru.datalink.protocol.node.extract;

import cn.guruguru.datalink.protocol.DataField;
import cn.guruguru.datalink.protocol.node.ExtractNode;
import cn.guruguru.datalink.protocol.transformation.WatermarkField;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Streaming Extract Node
 */
@EqualsAndHashCode(callSuper = true)
@Data
public abstract class CdcExtractNode extends ExtractNode {
    @Nullable
    @JsonProperty("watermarkField")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private WatermarkField watermarkField;

    public CdcExtractNode(String id,
                           String name,
                           List<DataField> fields,
                           @Nullable Map<String, String> properties,
                           @Nullable String filter,
                           @Nullable WatermarkField watermarkField) {
        super(id, name, fields, filter, properties);
        this.watermarkField = watermarkField;
    }
}
