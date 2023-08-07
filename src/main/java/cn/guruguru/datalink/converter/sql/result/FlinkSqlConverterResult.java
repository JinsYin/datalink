package cn.guruguru.datalink.converter.sql.result;

import cn.guruguru.datalink.converter.SqlConverterResult;
import com.google.common.base.Preconditions;
import lombok.Data;

@Data
public class FlinkSqlConverterResult implements SqlConverterResult {
    private String catalog;
    private String database;
    private String table;
    private String ddl;

    public FlinkSqlConverterResult(String catalog, String database, String table, String ddl) {
        this.catalog = Preconditions.checkNotNull(catalog, "catalog is null");
        this.database = Preconditions.checkNotNull(database, "database is null");
        this.table = Preconditions.checkNotNull(table, "table is null");
        this.ddl = Preconditions.checkNotNull(ddl, "ddl is null");
    }

    @Override
    public String getTableIdentifier() {
        return String.format("`%s`.`%s`.`%s`", catalog, database, table);
    }
}
