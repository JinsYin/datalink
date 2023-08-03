package cn.guruguru.datalink.protocol.field;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Fields or Function Parameters
 *
 * @see org.apache.inlong.sort.protocol.transformation.FunctionParam
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DataField.class, name = DataField.TYPE),
        @JsonSubTypes.Type(value = MetaField.class, name = MetaField.TYPE),
        @JsonSubTypes.Type(value = ConstantField.class, name = ConstantField.TYPE),
        @JsonSubTypes.Type(value = StringConstantField.class, name = StringConstantField.TYPE),
        @JsonSubTypes.Type(value = TimeUnitConstantField.class, name = TimeUnitConstantField.TYPE),
})
public interface Field {
    /**
     * Function param name
     *
     * @return The name of this function param
     */
    @JsonIgnore
    String getName();

    /**
     * Format used for sql
     *
     * @return The format value in sql
     */
    String format();
}
