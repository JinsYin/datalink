package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.converter.enums.JdbcDialect;

public interface SqlConverterResult {

    /**
     * Get JDBC dialect
     *
     * @return JDBC dialect
     */
    JdbcDialect getDialect();

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
     * Get database identifier;
     *
     * @return database identifier
     */
    String getDatabaseIdentifier();

    /**
     * Get table identifier
     *
     * @return table identifier
     */
    String getTableIdentifier();

    /**
     * Get CREATE-DATABASE statement
     *
     * @return a CREATE-DATABASE statement
     */
    default String getCreateDatabaseSql() {
        return String.format("CREATE DATABASE IF NOT EXISTS %s;", getDatabaseIdentifier());
    }

    /**
     * Get CREATE-TABLE statement
     *
     * @return a CREATE-TABLE statement
     */
    String getCreateTableSql();

    /**
     * Get DDL SQLs
     *
     * @return DDL SQLs
     */
    default String getSqls() {
        return getCreateDatabaseSql() + "\n\n" + getCreateTableSql();
    }
}
