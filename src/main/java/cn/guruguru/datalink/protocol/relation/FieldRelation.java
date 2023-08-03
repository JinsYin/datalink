package cn.guruguru.datalink.protocol.relation;

import cn.guruguru.datalink.protocol.field.DataField;
import cn.guruguru.datalink.protocol.field.Field;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.io.Serializable;

/**
 * Field Relation
 *
 * @see org.apache.inlong.sort.protocol.transformation.FieldRelation
 */
@JsonTypeName("FieldRelation")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@Data
@NoArgsConstructor
public class FieldRelation implements Serializable {
    @JsonProperty("inputField")
    private Field inputField;
    @JsonProperty("outputField")
    private DataField outputField;

    @JsonCreator
    public FieldRelation(@JsonProperty("inputField") DataField inputField,
                         @JsonProperty("outputField") DataField outputField) {
        this.inputField = inputField;
        this.outputField = outputField;
    }
}
