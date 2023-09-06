package cn.guruguru.datalink.type.definition;

import cn.guruguru.datalink.protocol.field.DataType;

/**
 * Flink Data Types
 *
 * @see org.apache.flink.table.api.DataTypes
 * @see org.apache.inlong.sort.formats.common.FormatInfo
 * @see org.apache.inlong.sort.formats.common.TypeInfo
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/">Data Types</a>
 */
public class FlinkDataTypes {
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

    public static DataType INTERVAL() {
        return new DataType("INTERVAL");
    }

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
}
