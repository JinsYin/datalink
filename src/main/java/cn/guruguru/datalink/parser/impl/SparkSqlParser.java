package cn.guruguru.datalink.parser.impl;

import cn.guruguru.datalink.type.converter.DataTypeConverter;
import cn.guruguru.datalink.type.converter.SparkDataTypeConverter;
import lombok.extern.slf4j.Slf4j;

/**
 * Spark sql parser
 */
@Slf4j
public class SparkSqlParser extends AbstractSqlParser {

    @Override
    public DataTypeConverter<String> getTypeConverter() {
        return new SparkDataTypeConverter();
    }
}
