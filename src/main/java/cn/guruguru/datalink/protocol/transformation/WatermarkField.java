package cn.guruguru.datalink.protocol.transformation;

import cn.guruguru.datalink.protocol.DataField;
import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.protocol.transformation.FunctionParam;
import org.apache.inlong.sort.protocol.transformation.StringConstantParam;
import org.apache.inlong.sort.protocol.transformation.TimeUnitConstantParam;

import java.util.Arrays;
import java.util.List;

/**
 * Watermark Field
 *
 * @see org.apache.inlong.sort.protocol.transformation.WatermarkField
 */
@JsonTypeName("watermark")
@Data
@NoArgsConstructor
public class WatermarkField {

    @JsonProperty("timeAttr")
    private DataField timeAttr;
    @JsonProperty("interval")
    private StringConstantParam interval;
    @JsonProperty("timeUnit")
    private TimeUnitConstantParam timeUnit;

    @JsonCreator
    public WatermarkField(@JsonProperty("timeAttr") DataField timeAttr,
                          @JsonProperty("interval") StringConstantParam interval,
                          @JsonProperty("timeUnit") TimeUnitConstantParam timeUnit) {
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

    public List<FunctionParam> getParams() {
        return Arrays.asList(timeAttr, interval, timeUnit);
    }

    public String getName() {
        return "WATERMARK";
    }
}
