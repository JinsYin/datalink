package cn.guruguru.datalink.converter.table;

import cn.guruguru.datalink.protocol.field.FieldFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

@EqualsAndHashCode(callSuper = true)
@Data
public class TableColumn extends FieldFormat {
    private String column;
    private String comment;

    @JsonCreator
    public TableColumn(
            @JsonProperty("column") String column,
            @JsonProperty("type") String type,
            @Nullable @JsonProperty("precision") Integer precision,
            @Nullable @JsonProperty("scale") Integer scale,
            @Nullable @JsonProperty("column") String comment) {
        super(type, precision, scale);
        this.column = column;
        this.comment = comment;
    }
}
