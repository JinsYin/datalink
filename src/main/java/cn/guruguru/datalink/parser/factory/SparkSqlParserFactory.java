package cn.guruguru.datalink.parser.factory;

import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.impl.SparkSqlParser;

public class SparkSqlParserFactory implements ParserFactory {
    /**
     * Creates a {@link SparkSqlParser}
     */
    @Override
    public Parser createParser() {
        return new SparkSqlParser();
    }
}
