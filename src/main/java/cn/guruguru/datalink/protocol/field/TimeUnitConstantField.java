package cn.guruguru.datalink.protocol.field;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nonnull;

/**
 * Time Unit Constant Field
 *
 * @see org.apache.inlong.sort.protocol.transformation.TimeUnitConstantParam
 */
@EqualsAndHashCode(callSuper = true)
@JsonTypeName("timeUnitConstantField")
@Data
@NoArgsConstructor
public class TimeUnitConstantField extends ConstantField {
    private static final long serialVersionUID = 659127597540343115L;

    @JsonProperty("timeUnit")
    private TimeUnit timeUnit;

    /**
     * TimeUnitConstantParam constructor
     *
     * @param timeUnit It is used to store time unit constant value
     */
    @JsonCreator
    public TimeUnitConstantField(@JsonProperty("timeUnit") @Nonnull TimeUnitConstantField.TimeUnit timeUnit) {
        super(Preconditions.checkNotNull(timeUnit, "timeUnit is null").name());
        this.timeUnit = timeUnit;
    }

    @Override
    public String getName() {
        return "timeUnitConstant";
    }

    @Override
    public String format() {
        return getValue().toString();
    }

    /**
     * The TimeUnit class defines an enumeration of time units
     */
    public enum TimeUnit {
        /**
         * Time unit for second
         */
        SECOND,
        /**
         * Time unit for minute
         */
        MINUTE,
        /**
         * Time unit for hour
         */
        HOUR
    }
}
