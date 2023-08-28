package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.converter.enums.JdbcDialect;
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
    List<T> toEngineDDL(JdbcDialect dialect, List<TableSchema> tableSchemas) throws RuntimeException;
}
