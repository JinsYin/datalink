package cn.guruguru.datalink.type.definition;

public interface DataTypes {

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
}
