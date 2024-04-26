package cn.guruguru.datalink.parser.factory;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.parser.EngineType;
import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.impl.FlinkSqlParser;
import cn.guruguru.datalink.parser.impl.SparkSqlParser;

/**
 * Simple Parser Factory designed based on the Simple Factory Pattern
 *
 * Usages:
 * <pre>
 *     Parser parser = SimpleParserFactory.createParser(EngineType.SPARK_SQL);
 *     ParseResult parseResult = parser.parser(pipeline);
 * </pre>
 */
public class SimpleParserFactory {

    /**
     * Creates a sql parser based on the type of computing engine
     *
     * @param engineType engine type
     * @return a concrete sql parser
     */
    public static Parser createParser(EngineType engineType) {
        switch (engineType) {
            case SPARK_SQL:
                return new SparkSqlParser();
            case FLINK_SQL:
                return new FlinkSqlParser();
            default:
                throw new UnsupportedEngineException("Unsupported engine：" + engineType);
        }
    }

    public static Parser of(EngineType engineType) {
        return createParser(engineType);
    }
}
