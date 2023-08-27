package cn.guruguru.datalink.converter;

public interface SqlConverterResult {
    /**
     * Get catalog name
     *
     * @return catalog name
     */
    String getCatalog();

    /**
     * Get database name
     *
     * @return database name
     */
    String getDatabase();

    /**
     * Get table name
     *
     * @return table name
     */
    String getTable();

    /**
     * Get table identifier
     *
     * @return table identifier
     */
    String getTableIdentifier();

    /**
     * Get converter result
     *
     * @return converter result
     */
    String getConverterResult();
}
