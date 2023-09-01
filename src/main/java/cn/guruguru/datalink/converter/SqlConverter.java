package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.converter.enums.JdbcDialect;
import cn.guruguru.datalink.converter.table.CaseStrategy;
import cn.guruguru.datalink.converter.table.DatabaseTableAffix;
import cn.guruguru.datalink.converter.table.TableDuplicateStrategy;
import cn.guruguru.datalink.converter.table.TableSchema;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

public interface SqlConverter<T> extends Serializable {

    /**
     * Convert data retrieved from data source DDL to engine DDL
     *
     * @param dialect data source type
     * @param catalog data catalog
     * @param database database
     * @param sqls SQLs from Data Source, non CREATE-TABLE statements will be ignored
     * @return DDL list
     */
    List<T> toEngineDDL(JdbcDialect dialect, String catalog, @Nullable String database, String sqls) throws RuntimeException;

    /**
     * Convert to engine SQL
     *
     * @param dialect dialect
     * @param tableSchemas table schema list
     * @return DDL List
     */
    default List<T> toEngineDDL(JdbcDialect dialect, List<TableSchema> tableSchemas) throws RuntimeException {
        return this.toEngineDDL(
                dialect,
                tableSchemas,
                null,
                null,
                TableDuplicateStrategy.IGNORE,
                CaseStrategy.SAME_NAME);
    }

    /**
     * Convert to engine SQL
     *
     * @param dialect dialect
     * @param tableSchemas table schema list
     * @param databaseAffix prefix or suffix of database name
     * @param tableAffix prefix or suffix of table name
     * @param tableDuplicateStrategy table name duplicate strategy
     * @param caseStrategy case strategy for database name, table name and field name
     * @return DDL list
     */
    List<T> toEngineDDL(JdbcDialect dialect,
                        List<TableSchema> tableSchemas,
                        DatabaseTableAffix databaseAffix,
                        DatabaseTableAffix tableAffix,
                        TableDuplicateStrategy tableDuplicateStrategy,
                        CaseStrategy caseStrategy) throws RuntimeException;
}
