package cn.guruguru.datalink.protocol.field;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * String constant field
 *
 * @see org.apache.inlong.sort.protocol.transformation.StringConstantParam
 */
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@JsonTypeName("StringConstantField")
@Data
public class StringConstantField extends ConstantField {
    private static final long serialVersionUID = 1779799826901827567L;

    /**
     * StringConstantParam constructor
     *
     * @param value It is used to store string constant value
     */
    public StringConstantField(@JsonProperty("value") String value) {
        super(value);
    }

    @Override
    public String format() {
        String value = getValue().toString();
        if (!value.startsWith("'") && !value.startsWith("\"")) {
            return String.format("'%s'", value);
        }
        return value;
    }
}
