package cn.guruguru.datalink.type.definition;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.TimestampType;

import java.util.Collections;
import java.util.List;

/**
 * Data types for Spark SQL
 *
 * @see <a href="https://spark.apache.org/docs/latest/sql-ref-datatypes.html">Data types</a>
 */
public class SparkDataTypes implements DataTypes {
    @Override
    public List<String> getAllTypes() {
        return Collections.emptyList();
    }

    // ~ For classification -------------------------------

    /**
     * Check if a type is a numerical type
     *
     * @see
     * @param typeString a type string for Spark
     * @return true or false
     */
    @Override
    public boolean isNumericType(String typeString) {
        DataType dataType = DateType.fromDDL("`any_field` " + typeString);
        return isNumericType(dataType);
    }

    /**
     * Check if a type is a numerical type
     *
     * @param dataType a data type for Spark
     * @return true or false
     */
    public boolean isNumericType(DataType dataType) {
        return dataType instanceof NumericType;
    }

    /**
     * Check if a type is a date or time type
     *
     * @param typeString a type string for Spark
     * @return true or false
     */
    @Override
    public boolean isDatetimeType(String typeString) {
        DataType dataType = DateType.fromDDL("`any_field` " + typeString);
        return isDatetimeType(dataType);
    }

    /**
     * Check if a type is a date or time type
     *
     * @param dataType a data type for Spark
     * @return true or false
     */
    public boolean isDatetimeType(DataType dataType) {
        return dataType instanceof DateType
                || dataType instanceof TimestampType
                || dataType.getClass().getSimpleName().equals("TimestampNTZType");
    }

    /**
     * Check if a type is a character type
     *
     * @param typeString a type string for various engines
     * @return true or false
     */
    public boolean isCharacterType(String typeString) {
        typeString = String.valueOf(typeString).toUpperCase();
        return typeString.equals("CHAR")
                || typeString.equals("VARCHAR")
                || typeString.equals("STRING");
    }
}
