package cn.guruguru.datalink.ddl.table;

import cn.guruguru.datalink.protocol.field.FieldFormat;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
public class TableField extends FieldFormat {
    @JsonProperty("name")
    private String name;
    @JsonProperty("comment")
    private String comment;
    @JsonProperty("isPrimaryKey")
    private boolean isPrimaryKey;
    @JsonProperty("isPartitionKey")
    private boolean isPartitionKey;
    @JsonProperty("isNotNull")
    private boolean isNotNull;

    @JsonCreator
    public TableField(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @Nullable @JsonProperty("precision") Integer precision,
            @Nullable @JsonProperty("scale") Integer scale,
            @Nullable @JsonProperty("comment") String comment,
            @JsonProperty("isPrimaryKey") boolean isPrimaryKey,
            @JsonProperty("isPartitionKey") boolean isPartitionKey,
            @JsonProperty("isNotNull") boolean isNotNull) {
        super(type, precision, scale);
        this.name = name;
        this.comment = comment;
        this.isPrimaryKey = isPrimaryKey;
        this.isPartitionKey = isPartitionKey;
        this.isNotNull = isNotNull;
    }

    public TableField(@JsonProperty("name") String name,
                      @JsonProperty("type") String type,
                      @Nullable @JsonProperty("precision") Integer precision,
                      @Nullable @JsonProperty("scale") Integer scale,
                      @Nullable @JsonProperty("comment") String comment) {
        this(name, type, precision, scale, comment, false, false, false);
    }
}
