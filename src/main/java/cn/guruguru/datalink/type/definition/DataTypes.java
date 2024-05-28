package cn.guruguru.datalink.type.definition;

import cn.guruguru.datalink.protocol.field.DataType;

import java.util.List;

public interface DataTypes {

    /**
     * Get all data types of the engine
     *
     * @return a list of data type
     */
    List<String> getAllTypes();

    /**
     * Return default value based on provided type
     *
     * @param dataType data type
     * @return default value
     */
    default Object getDefaultValue(DataType dataType) {
        String fullType = dataType.getType();
        String typeName = fullType.replaceAll("(\\S)(\\(.*\\))?", "$1");
        if (isNumericType(typeName)) {
            return 0;
        } else if (isCharacterType(typeName)) {
            return "\"\"";
        } else {
            return "NULL";
        }
    }

    // ~ --------------------------------------------------
    // For classification, e.g. determining incremental columns
    // ~ --------------------------------------------------

    /**
     * Check if a type is a numerical type
     *
     * @param typeString a type string for various engines
     * @return true or false
     */
    boolean isNumericType(String typeString);

    /**
     * Check if a type is a numerical type
     *
     * @param typeString a type string for various engines
     * @return true or false
     */
    boolean isDatetimeType(String typeString);

    /**
     * Check if a type is a character type
     *
     * @param typeString a type string for various engines
     * @return true or false
     */
    boolean isCharacterType(String typeString);
}
