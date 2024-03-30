package cn.guruguru.datalink.type.converter.factory;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.type.converter.DataTypeConverter;
import cn.guruguru.datalink.type.converter.FlinkDataTypeConverter;
import cn.guruguru.datalink.type.converter.SparkDataTypeConverter;

/**
 * Simple Converter Factory for creating a {@link
 * DataTypeConverter}
 *
 * <p>Usages:
 * <pre>
 *     DataTypeConverter typeConverter = SimpleTypeConverterFactory.createInstance("Spark");
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
    public static DataTypeConverter createInstance(String engineType) {
        switch (engineType) {
            case "Spark":
                return new SparkDataTypeConverter();
            case "Flink":
                return new FlinkDataTypeConverter();
            default:
                throw new UnsupportedEngineException("Unsupported engine:" + engineType);
        }
    }

    public static DataTypeConverter of(String engineType) {
        return createInstance(engineType);
    }
}
