package cn.guruguru.datalink.converter;

public interface SqlConverterResult {
    /**
     * Get catalog name
     */
    String getCatalog();

    /**
     * Get database name
     */
    String getDatabase();

    /**
     * Get table name
     */
    String getTable();

    /**
     * Get table identifier
     */
    String getTableIdentifier();

    /**
     * Get converter result
     */
    String getConverterResult();
}
