package cn.guruguru.datalink.protocol.field;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.inlong.sort.protocol.transformation.TimeUnitConstantParam;

import java.util.Arrays;
import java.util.List;

/**
 * Watermark Field
 *
 * @see org.apache.inlong.sort.protocol.transformation.WatermarkField
 */
@JsonTypeName(WatermarkField.TYPE)
@Data
@NoArgsConstructor
public class WatermarkField {

    public static final String TYPE = "watermark";

    @JsonProperty("timeAttr")
    private DataField timeAttr;
    @JsonProperty("interval")
    private StringConstantField interval;
    @JsonProperty("timeUnit")
    private TimeUnitConstantField timeUnit;

    @JsonCreator
    public WatermarkField(@JsonProperty("timeAttr") DataField timeAttr,
                          @JsonProperty("interval") StringConstantField interval,
                          @JsonProperty("timeUnit") TimeUnitConstantField timeUnit) {
        this.timeAttr = Preconditions.checkNotNull(timeAttr, "timeAttr is null");
        this.interval = interval;
        this.timeUnit = timeUnit;
    }

    public WatermarkField(@JsonProperty("timeAttr") DataField timeAttr) {
        this(timeAttr, null, null);
    }

    public String format() {
        if (interval == null) {
            return String.format("%s FOR %s AS %s", getName(), timeAttr.format(), timeAttr.format());
        }
        if (timeUnit == null) {
            return String.format("%s FOR %s AS %s - INTERVAL %s %s", getName(), timeAttr.format(),
                    timeAttr.format(), interval.format(), TimeUnitConstantParam.TimeUnit.SECOND.name());
        }
        return String.format("%s FOR %s AS %s - INTERVAL %s %s", getName(), timeAttr.format(),
                timeAttr.format(), interval.format(), timeUnit.format());
    }

    public List<Field> getParams() {
        return Arrays.asList(timeAttr, interval, timeUnit);
    }

    public String getName() {
        return "WATERMARK";
    }
}
