package cn.guruguru.datalink.converter;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.calcite.sql.parser.SqlParseException;

import javax.annotation.Nullable;
import java.io.Serializable;

public interface SqlConverter extends Serializable {

    /**
     * Convert data retrieved from data source DDL to engine DDL
     *
     * @param sourceType data source type
     * @param catalog data catalog
     * @param database database
     * @param ddl DDL SQL from Data Source
     */
    String toEngineDDL(String sourceType, String catalog, @Nullable String database, String ddl) throws JSQLParserException, SqlParseException;
}
