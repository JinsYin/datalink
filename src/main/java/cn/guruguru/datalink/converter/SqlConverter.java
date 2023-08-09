package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.converter.enums.DDLDialect;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.calcite.sql.parser.SqlParseException;

import javax.annotation.Nullable;
import java.io.Serializable;

public interface SqlConverter<T> extends Serializable {

    /**
     * Convert data retrieved from data source DDL to engine DDL
     *
     * @param dialect data source type
     * @param catalog data catalog
     * @param database database
     * @param ddl DDL SQL from Data Source
     */
    T toEngineDDL(DDLDialect dialect, String catalog, @Nullable String database, String ddl) throws RuntimeException;
}
