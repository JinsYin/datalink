package cn.guruguru.datalink.parser;

/**
 * Parser interface
 *
 * @see org.apache.inlong.sort.parser.Parser
 */
public interface Parser {
    /**
     * Parse data model to generate flink sql or flink stream api
     *
     * @return ParseResult the result of parsing
     */
    ParseResult parse();
}
