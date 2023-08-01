package cn.guruguru.datalink.parser.result;

import cn.guruguru.datalink.parser.ParseResult;

/**
 * [TODO] Parser result for spark sql
 */
public class SparkSqlParseResult implements ParseResult {
    @Override
    public String getSqlScript() {
        throw new UnsupportedOperationException("Spark engine not supported");
    }
}
