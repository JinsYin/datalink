package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.parser.ParseResult;
import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.protocol.LinkInfo;

/**
 * [TODO] Spark sql parser
 */
public class SparkSqlParser implements Parser {
    @Override
    public ParseResult parse(LinkInfo linkInfo) {
        throw new UnsupportedEngineException("Spark engine not supported");
    }
}
