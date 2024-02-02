package cn.guruguru.datalink.parser;

import cn.guruguru.datalink.protocol.LinkInfo;

/**
 * Parser interface
 *
 * @see org.apache.inlong.sort.parser.Parser
 */
public interface Parser {
    /**
     * Parse data model to generate flink sql or flink stream api
     *
     * @param linkInfo a {@link LinkInfo}
     * @return ParseResult the result of parsing
     */
    ParseResult parse(LinkInfo linkInfo);
}
