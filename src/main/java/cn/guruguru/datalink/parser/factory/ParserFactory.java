package cn.guruguru.datalink.parser.factory;

import cn.guruguru.datalink.parser.Parser;

/**
 * Parser Factory designed based on the Factory Method Pattern
 *
 * Usages of the FlinkSqlParserFactory:
 * <pre>
 *     ParserFactory flinkSqlParserFactory = new FlinkSqlParserFactory();
 *     Parser flinkSqlParser = flinkSqlParserFactory.createParser();
 *     ParseResult flinkSqlResult = flinkSqlParser.parse(linkInfo);
 * </pre>
 *
 * Usages of the SparkSqlParserFactory:
 * <pre>
 *     ParserFactory sparkSqlParserFactory = new SparkSqlParserFactory();
 *     Parser sparkSqlParser = sparkSqlParserFactory.createParser();
 *     ParseResult sparkSqlResult = sparkSqlParser.parse(linkInfo);
 * </pre>
 */
public interface ParserFactory {
    /**
     * Creates a SQL parser
     *
     * @return a SQL parser
     */
    Parser createParser();
}
