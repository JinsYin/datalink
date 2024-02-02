package cn.guruguru.datalink.parser.factory;

import cn.guruguru.datalink.parser.Parser;
import cn.guruguru.datalink.parser.impl.FlinkSqlParser;
import lombok.Data;

@Data
public class FlinkSqlParserFactory implements ParserFactory {
    /**
     * Creates a {@link FlinkSqlParser}
     */
    @Override
    public Parser createParser() {
        return new FlinkSqlParser();
    }
}
