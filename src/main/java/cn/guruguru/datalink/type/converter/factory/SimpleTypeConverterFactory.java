package cn.guruguru.datalink.type.converter.factory;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.parser.EngineType;
import cn.guruguru.datalink.type.converter.DataTypeConverter;
import cn.guruguru.datalink.type.converter.FlinkDataTypeConverter;
import cn.guruguru.datalink.type.converter.SparkDataTypeConverter;

/**
 * Simple Converter Factory for creating a {@link
 * DataTypeConverter}
 *
 * <p>Usages:
 * <pre>
 *     DataTypeConverter typeConverter = SimpleTypeConverterFactory.createInstance(EngineType.SPARK_SQL);
 *     String fieldType = parser.toEngineType(nodeType, dataType);
 * </pre>
 */
public class SimpleTypeConverterFactory {

    /**
     * Creates a data converter based on the type of computing engine
     *
     * @param engineType engine type
     * @return a concrete type converter
     */
    public static DataTypeConverter createInstance(EngineType engineType) {
        switch (engineType) {
            case SPARK_SQL:
                return new SparkDataTypeConverter();
            case FLINK_SQL:
                return new FlinkDataTypeConverter();
            default:
                throw new UnsupportedEngineException("Unsupported engine:" + engineType);
        }
    }

    public static DataTypeConverter of(EngineType engineType) {
        return createInstance(engineType);
    }
}
