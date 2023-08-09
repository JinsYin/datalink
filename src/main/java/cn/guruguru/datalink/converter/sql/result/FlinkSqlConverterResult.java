package cn.guruguru.datalink.converter.sql.result;

import cn.guruguru.datalink.converter.SqlConverterResult;
import com.google.common.base.Preconditions;

public class FlinkSqlConverterResult implements SqlConverterResult {
    private final String catalog;
    private final String database;
    private final String table;
    private final String ddl;

    public FlinkSqlConverterResult(String catalog, String database, String table, String ddl) {
        this.catalog = Preconditions.checkNotNull(catalog, "catalog is null");
        this.database = Preconditions.checkNotNull(database, "database is null");
        this.table = Preconditions.checkNotNull(table, "table is null");
        this.ddl = Preconditions.checkNotNull(ddl, "ddl is null");
    }

    @Override
    public String getCatalog() {
        return this.catalog;
    }

    @Override
    public String getDatabase() {
        return this.database;
    }

    @Override
    public String getTable() {
        return this.table;
    }

    @Override
    public String getTableIdentifier() {
        return String.format("`%s`.`%s`.`%s`", catalog, database, table);
    }

    @Override
    public String getConverterResult() {
        return this.ddl;
    }
}
