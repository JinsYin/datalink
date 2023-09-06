package cn.guruguru.datalink.type.definition;

import cn.guruguru.datalink.protocol.field.FieldFormat;

/**
 * Flink Data Types
 *
 * @see org.apache.flink.table.api.DataTypes
 * @see org.apache.inlong.sort.formats.common.FormatInfo
 * @see org.apache.inlong.sort.formats.common.TypeInfo
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/types/">Data Types</a>
 */
public class FlinkDataTypes {
    public static FieldFormat CHAR() {
        return new FieldFormat("CHAR");
    }

    public static FieldFormat VARCHAR() {
        return new FieldFormat("VARCHAR");
    }

    public static FieldFormat STRING() {
        return new FieldFormat("STRING");
    }

    public static FieldFormat BOOLEAN() {
        return new FieldFormat("BOOLEAN");
    }

    public static FieldFormat BINARY() {
        return new FieldFormat("BINARY");
    }

    public static FieldFormat VARBINARY() {
        return new FieldFormat("VARBINARY");
    }
    public static FieldFormat BYTES() {
        return new FieldFormat("BYTES");
    }

    public static FieldFormat DECIMAL() {
        return new FieldFormat("DECIMAL"); // Supports fixed precision and scale.
    }

    public static FieldFormat TINYINT() {
        return new FieldFormat("TINYINT");
    }

    public static FieldFormat SMALLINT() {
        return new FieldFormat("SMALLINT");
    }

    public static FieldFormat INTEGER() {
        return new FieldFormat("INTEGER");
    }
    public static FieldFormat BIGINT() {
        return new FieldFormat("BIGINT");
    }

    public static FieldFormat FLOAT() {
        return new FieldFormat("FLOAT");
    }
    public static FieldFormat DOUBLE() {
        return new FieldFormat("DOUBLE");
    }

    public static FieldFormat DATE() {
        return new FieldFormat("DATE");
    }

    public static FieldFormat TIME() {
        return new FieldFormat("TIME"); // Supports only a precision of 0.
    }

    public static FieldFormat TIMESTAMP() {
        return new FieldFormat("TIMESTAMP");
    }

    public static FieldFormat TIMESTAMP_LTZ() {
        return new FieldFormat("TIMESTAMP_LTZ");
    }

    public static FieldFormat INTERVAL() {
        return new FieldFormat("INTERVAL");
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
