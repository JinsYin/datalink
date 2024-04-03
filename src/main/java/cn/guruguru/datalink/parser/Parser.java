package cn.guruguru.datalink.parser;

import cn.guruguru.datalink.parser.result.ParseResult;
import cn.guruguru.datalink.protocol.Pipeline;

/**
 * Parser interface
 *
 * @see org.apache.inlong.sort.parser.Parser
 */
public interface Parser {
    /**
     * Parse data model to generate flink sql or flink stream api
     *
     * @param pipeline a {@link Pipeline}
     * @return ParseResult the result of parsing
     */
    ParseResult parse(Pipeline pipeline);
}
