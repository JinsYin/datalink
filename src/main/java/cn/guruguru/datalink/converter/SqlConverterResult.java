package cn.guruguru.datalink.converter;

public interface SqlConverterResult {
    /**
     * Get catalog name
     */
    String getCatalogName();

    /**
     * Get database name
     */
    String getDatabaseName();

    /**
     * Get table name
     */
    String getTableName();

    /**
     * Get table identifier
     */
    String getTableIdentifier();

    /**
     * Get converter result
     */
    String getConverterResult();
}
