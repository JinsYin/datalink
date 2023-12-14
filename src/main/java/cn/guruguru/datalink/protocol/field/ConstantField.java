package cn.guruguru.datalink.protocol.field;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConstantField.class, name = ConstantField.TYPE),
        @JsonSubTypes.Type(value = TimeUnitConstantField.class, name = TimeUnitConstantField.TYPE),
        @JsonSubTypes.Type(value = StringConstantField.class, name = StringConstantField.TYPE)
})
@NoArgsConstructor
@Data
public class ConstantField implements Field, Serializable {
    private static final long serialVersionUID = 7216146498324134122L;
    public static final String TYPE = "ConstantField";

    @JsonProperty("value")
    private Object value;

    /**
     * ConstantParam constructor
     *
     * @param value It is used to store constant value
     */
    @JsonCreator
    public ConstantField(@JsonProperty("value") Object value) {
        this.value = Preconditions.checkNotNull(value, "value is null");
    }

    @Override
    public String getName() {
        return "constant";
    }

    @Override
    public String format() {
        return value.toString();
    }
}
