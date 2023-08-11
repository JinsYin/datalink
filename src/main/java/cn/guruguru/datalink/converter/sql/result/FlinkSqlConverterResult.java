package cn.guruguru.datalink.converter.sql.result;

import cn.guruguru.datalink.converter.SqlConverterResult;
import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlinkSqlConverterResult implements SqlConverterResult {
    private String catalog;
    private String database;
    private String table;
    private final String ddl;

    public FlinkSqlConverterResult(String catalog, String database, String table, String ddl) {
        this.catalog = Preconditions.checkNotNull(catalog, "catalog is null");
        this.database = Preconditions.checkNotNull(database, "database is null");
        this.table = Preconditions.checkNotNull(table, "table is null");
        this.ddl = Preconditions.checkNotNull(ddl, "ddl is null");
    }

    public FlinkSqlConverterResult(String tableIdentifier, String ddl) {
        Preconditions.checkNotNull(tableIdentifier, "tableIdentifier is null");
        Matcher matcher = Pattern.compile("`(.*)`.`(.*)`.`(.*)`").matcher(tableIdentifier);
        if (matcher.matches()) {
            String catalog = matcher.group(1);
            String database = matcher.group(2);
            String table = matcher.group(3);
            this.catalog = catalog;
            this.database = database;
            this.table = table;
        } else {
            throw new IllegalArgumentException("table identifier format is incorrect");
        }
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
