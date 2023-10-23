package cn.guruguru.datalink.protocol.field;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Data field type
 *
 * @see org.apache.flink.table.api.DataTypes
 * @see org.apache.inlong.sort.formats.common.FormatInfo
 * @see org.apache.inlong.sort.formats.common.TypeInfo
 */
@Data
@NoArgsConstructor
public class DataType {
    @JsonProperty("type")
    private String type;
    /**
     * formatted display name, like {@code DECIMAL(10, 2)}
     */
    @Nullable
    @JsonProperty("name")
    private String name;
    @JsonProperty("precision")
    private Integer precision;
    @JsonProperty("scale")
    private Integer scale;
    // @JsonProperty("timeZone")
    // @Nullable
    // private String timeZone;

    @JsonCreator
    public DataType(@Nonnull @JsonProperty("type") String type,
                    @Nullable @JsonProperty("precision") Integer precision,
                    @Nullable @JsonProperty("scale") Integer scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.name = toString();
        //if (!type.contains("(") && precision != null && scale != null) {
        //    this.displayType = String.format("%s(%s,%s)", type, precision, scale);
        //} else if (!type.contains("(") && precision != null) {
        //    this.displayType = String.format("%s(%s)", type, precision);
        //} else {
        //    this.displayType = type;
        //}
    }

    public DataType(@JsonProperty("type") String type) {
        this.type = type;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(type);
        if (precision != null) {
            sb.append("(").append(precision);
            if (scale != null) {
                sb.append(", ").append(scale);
            }
            sb.append(")");
        }
        return sb.toString();
    }
}
