package cn.guruguru.datalink.type.definition;

import java.util.List;

public interface DataTypes {

    /**
     * Get all data types of the engine
     *
     * @return a list of data type
     */
    List<String> getAllTypes();

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
