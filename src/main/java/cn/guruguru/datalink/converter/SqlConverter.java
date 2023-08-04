package cn.guruguru.datalink.converter;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.calcite.sql.parser.SqlParseException;

import java.io.Serializable;

public interface SqlConverter extends Serializable {

    /**
     * Convert data retrieved from data source DDL to engine DDL
     *
     * @param sourceType data source type
     * @param ddl DDL SQL from Data Source
     */
    String toEngineDDL(String sourceType, String ddl) throws JSQLParserException, SqlParseException;
}
