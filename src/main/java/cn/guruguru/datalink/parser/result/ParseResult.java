package cn.guruguru.datalink.parser.result;

import java.io.Serializable;
import java.util.List;

/**
 * Parser result
 *
 * @see org.apache.inlong.sort.parser.result.ParseResult
 */
public interface ParseResult extends Serializable {

    /**
     * Generates a list of SQL statements that can be executed in an orderly manner
     *
     * @return a list of SQL statements
     */
    List<String> getSqlStatements();

    /**
     * Formats a set of SQL statements to a SQL script
     *
     * @return a formatted SQL Script
     */
    default String getSqlScript() {
        List<String> sqls = getSqlStatements();
        return String.join(";\n", sqls) + ";";
    }
}
