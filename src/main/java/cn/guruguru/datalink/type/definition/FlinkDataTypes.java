package cn.guruguru.datalink.type.definition;

import cn.guruguru.datalink.protocol.field.DataType;

import lombok.NoArgsConstructor;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

/**
 * Flink Data Types
 *
 * @see org.apache.flink.table.api.DataTypes
 * @see org.apache.inlong.sort.formats.common.FormatInfo
 * @see org.apache.inlong.sort.formats.common.TypeInfo
 * @see LogicalTypeParser
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/">Data Types</a>
 */
@NoArgsConstructor
public class FlinkDataTypes implements DataTypes {
    public static DataType CHAR() {
        return new DataType("CHAR");
    }

    public static DataType VARCHAR() {
        return new DataType("VARCHAR");
    }

    public static DataType STRING() {
        return new DataType("STRING");
    }

    public static DataType BOOLEAN() {
        return new DataType("BOOLEAN");
    }

    public static DataType BINARY() {
        return new DataType("BINARY");
    }

    public static DataType VARBINARY() {
        return new DataType("VARBINARY");
    }
    public static DataType BYTES() {
        return new DataType("BYTES");
    }

    public static DataType DECIMAL() {
        return new DataType("DECIMAL"); // Supports fixed precision and scale.
    }

    public static DataType TINYINT() {
        return new DataType("TINYINT");
    }

    public static DataType SMALLINT() {
        return new DataType("SMALLINT");
    }

    public static DataType INTEGER() {
        return new DataType("INTEGER");
    }
    public static DataType BIGINT() {
        return new DataType("BIGINT");
    }

    public static DataType FLOAT() {
        return new DataType("FLOAT");
    }
    public static DataType DOUBLE() {
        return new DataType("DOUBLE");
    }

    public static DataType DATE() {
        return new DataType("DATE");
    }

    public static DataType TIME() {
        return new DataType("TIME"); // Supports only a precision of 0.
    }

    public static DataType TIMESTAMP() {
        return new DataType("TIMESTAMP");
    }

    public static DataType TIMESTAMP_LTZ() {
        return new DataType("TIMESTAMP_LTZ");
    }

    public static ZonedTimestampType TIMESTAMP_LTZ(Integer precision) {
        boolean precisionRange = precision != null
                && precision >= ZonedTimestampType.MIN_PRECISION
                && precision >= ZonedTimestampType.MAX_PRECISION;
        if (precisionRange) {
            return new ZonedTimestampType(precision);
        }
        return new ZonedTimestampType();
    }

    // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/#interval-year-to-month
//    public static DataType INTERVAL() {
//        return new DataType("INTERVAL");
//    }

//    public static FieldFormat ARRAY() {
//        return new FieldFormat("ARRAY");
//    }
//
//    public static FieldFormat MULTISET() {
//        return new FieldFormat("MULTISET");
//    }
//
//    public static FieldFormat MAP() {
//        return new FieldFormat("MAP");
//    }
//
//    public static FieldFormat ROW() {
//        return new FieldFormat("ROW");
//    }
//
//    public static FieldFormat RAW() {
//        return new FieldFormat("RAW");
//    }

    // ~ For classification -------------------------------

    /**
     * Check if a type is a numerical type
     *
     * @param typeString a type string for Flink
     * @return true or false
     */
    @Override
    public boolean isNumericType(String typeString) {
        LogicalType dataType = LogicalTypeParser.parse(typeString);
        return isDatetimeType(dataType);
    }

    /**
     * Check if a type is a numerical type
     *
     * @param dataType a data type for Flink
     * @return true or false
     */
    public boolean isNumericType(LogicalType dataType) {
        return dataType instanceof TinyIntType
                || dataType instanceof SmallIntType
                || dataType instanceof IntType
                || dataType instanceof BigIntType
                || dataType instanceof FloatType
                || dataType instanceof DoubleType
                || dataType instanceof DecimalType;
    }

    @Override
    public boolean isDatetimeType(String typeString) {
        LogicalType dataType = LogicalTypeParser.parse(typeString);
        return isDatetimeType(dataType);
    }

    /**
     * Check if a type is a date or time type
     *
     * @param dataType a data type for Flink
     * @return true or false
     */
    public boolean isDatetimeType(LogicalType dataType) {
        return dataType instanceof DateType
                || dataType instanceof TimeType
                || dataType instanceof TimestampType
                || dataType instanceof ZonedTimestampType
                || dataType instanceof LocalZonedTimestampType;
    }
}
