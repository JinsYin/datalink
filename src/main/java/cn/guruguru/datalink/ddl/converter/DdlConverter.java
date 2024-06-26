package cn.guruguru.datalink.ddl.converter;

import cn.guruguru.datalink.ddl.table.JdbcDialect;
import cn.guruguru.datalink.ddl.table.CaseStrategy;
import cn.guruguru.datalink.ddl.table.Affix;
import cn.guruguru.datalink.ddl.table.TableDuplicateStrategy;
import cn.guruguru.datalink.ddl.table.TableSchema;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

public interface DdlConverter<T> extends Serializable {

    /**
     * Convert to engine SQL from table schemas
     *
     * @param dialect dialect
     * @param tableSchemas table schema list
     * @param databaseAffix prefix or suffix of database name
     * @param tableAffix prefix or suffix of table name
     * @param tableDuplicateStrategy table name duplicate strategy
     * @param caseStrategy case strategy for database name, table name and field name
     * @return SQL DDL
     */
    T convertSchema(JdbcDialect dialect,
                    List<TableSchema> tableSchemas,
                    Affix databaseAffix,
                    Affix tableAffix,
                    TableDuplicateStrategy tableDuplicateStrategy,
                    CaseStrategy caseStrategy) throws RuntimeException;

    /**
     * Convert to engine SQL from table schemas
     *
     * @param dialect dialect
     * @param tableSchemas table schema list
     * @return SQL DDL
     */
    default T convertSchema(JdbcDialect dialect, List<TableSchema> tableSchemas) throws RuntimeException {
        return this.convertSchema(
                dialect,
                tableSchemas,
                null,
                null,
                TableDuplicateStrategy.IGNORE,
                CaseStrategy.SAME_NAME);
    }

    /**
     * Convert data retrieved from data source DDL to engine DDL
     *
     * @param dialect data source type
     * @param targetCatalog target catalog
     * @param defaultDatabase default database, If database is set in SQL (like {@code CREATE TABLE `db1`.`tb1` (...)}),
     *                        it will be ignored
     * @param sql one or more SQL statements from Data Source, non-CREATE-TABLE statements will be ignored
     * @param caseStrategy case strategy
     * @return SQL DDL
     */
    T convertSql(JdbcDialect dialect,
                 String targetCatalog,
                 @Nullable String defaultDatabase,
                 String sql,
                 CaseStrategy caseStrategy) throws RuntimeException;

    /**
     * Convert data retrieved from data source DDL to engine DDL
     *
     * @param dialect data source type
     * @param targetCatalog target catalog
     * @param defaultDatabase default database, If database is set in SQL (like {@code CREATE TABLE `db1`.`tb1` (...)}),
     *                        it will be ignored
     * @param sql one or more SQL statements from Data Source, non-CREATE-TABLE statements will be ignored
     * @return SQL DDL
     */
    default T convertSql(JdbcDialect dialect,
                 String targetCatalog,
                 @Nullable String defaultDatabase,
                 String sql) throws RuntimeException {
        return this.convertSql(dialect, targetCatalog, defaultDatabase, sql, CaseStrategy.SAME_NAME);
    }
}
