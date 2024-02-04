package cn.guruguru.datalink.type.converter;

import cn.guruguru.datalink.protocol.field.DataType;

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.io.Serializable;

/**
 * Type converter interface
 */
public interface DataTypeConverter<T> extends Serializable {

    /**
     * Converts field type
     *
     * {@code org.apache.inlong.sort.formats.base.TableFormatUtils#deriveLogicalType(FormatInfo)}
     * @param nodeType node type
     * @param dataType source field
     * @return engine field
     */
    T toEngineType(String nodeType, DataType dataType);

    // ~ formats data types -------------------------------

    default DecimalType formatDecimalType(Integer precision, Integer scale) {
        boolean precisionRange = precision != null
                                 && precision >= DecimalType.MIN_PRECISION
                                 && precision <= DecimalType.MAX_PRECISION;
        if (precisionRange && scale != null) {
            return new DecimalType(precision, scale);
        } else if (precisionRange) {
            return new DecimalType(precision);
        } else {
            return new DecimalType();
        }
    }

    default TimeType formatTimeType(Integer precision) {
        boolean precisionRange = precision != null
                                 && precision >= TimeType.MIN_PRECISION
                                 && precision >= TimeType.MAX_PRECISION;
        if (precisionRange) {
            return new TimeType(precision);
        }
        return new TimeType();
    }

    default TimestampType formatTimestampType(Integer precision) {
        boolean precisionRange = precision != null
                                 && precision >= TimestampType.MIN_PRECISION
                                 && precision >= TimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new TimestampType(precision);
        }
        return new TimestampType();
    }

    default ZonedTimestampType formatZonedTimestampType(Integer precision) {
        boolean precisionRange = precision != null
                                 && precision >= ZonedTimestampType.MIN_PRECISION
                                 && precision >= ZonedTimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new ZonedTimestampType(precision);
        }
        return new ZonedTimestampType();
    }

    default LocalZonedTimestampType formatLocalZonedTimestampType(Integer precision) {
        boolean precisionRange = precision != null
                                 && precision >= LocalZonedTimestampType.MIN_PRECISION
                                 && precision >= LocalZonedTimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new LocalZonedTimestampType(precision);
        }
        return new LocalZonedTimestampType();
    }
}
