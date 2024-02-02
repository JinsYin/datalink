package cn.guruguru.datalink.parser.result;

/**
 * Parser result
 *
 * @see org.apache.inlong.sort.parser.result.ParseResult
 */
public interface ParseResult {

    /**
     * Generate corresponding SQL scripts for different engines
     *
     * @return SQL Script
     */
    String getSqlScript();
}
