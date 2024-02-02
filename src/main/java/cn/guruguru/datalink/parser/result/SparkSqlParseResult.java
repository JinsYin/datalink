package cn.guruguru.datalink.parser.result;

import cn.guruguru.datalink.exception.UnsupportedEngineException;

/**
 * [TODO] Parser result for spark sql
 */
public class SparkSqlParseResult implements ParseResult {
    @Override
    public String getSqlScript() {
        throw new UnsupportedEngineException("Spark engine not supported");
    }
}
