package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.converter.enums.DDLDialect;
import cn.guruguru.datalink.converter.table.TableSchema;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.calcite.sql.parser.SqlParseException;

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
     */
    List<T> toEngineDDL(DDLDialect dialect, String catalog, @Nullable String database, String sqls) throws RuntimeException;

    /**
     * Convert to engine SQL
     *
     * @param dialect dialect
     * @param tableSchemas table schema list
     * @return DDLs
     */
    List<T> toEngineDDL(DDLDialect dialect, List<TableSchema> tableSchemas) throws RuntimeException;
}
