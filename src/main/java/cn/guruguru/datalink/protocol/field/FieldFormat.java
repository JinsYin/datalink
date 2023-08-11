package cn.guruguru.datalink.protocol.field;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

@Data
@NoArgsConstructor
public class FieldFormat {
    @JsonProperty("type")
    private String type;
    @JsonProperty("precision")
    private Integer precision;
    @JsonProperty("scale")
    private Integer scale;
    // @JsonProperty("timeZone")
    // @Nullable
    // private String timeZone;

    @JsonCreator
    public FieldFormat(@JsonProperty("type") String type,
                       @Nullable @JsonProperty("precision") Integer precision,
                       @Nullable @JsonProperty("scale") Integer scale) {
        this.type = type;
        this.precision = precision;
        this.scale = scale;
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
