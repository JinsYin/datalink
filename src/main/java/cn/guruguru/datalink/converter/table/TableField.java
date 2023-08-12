package cn.guruguru.datalink.converter.table;

import cn.guruguru.datalink.protocol.field.FieldFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

@EqualsAndHashCode(callSuper = true)
@Data
public class TableField extends FieldFormat {
    @JsonProperty("name")
    private String name;
    @JsonProperty("comment")
    private String comment;

    @JsonCreator
    public TableField(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @Nullable @JsonProperty("precision") Integer precision,
            @Nullable @JsonProperty("scale") Integer scale,
            @Nullable @JsonProperty("column") String comment) {
        super(type, precision, scale);
        this.name = name;
        this.comment = comment;
    }
}
