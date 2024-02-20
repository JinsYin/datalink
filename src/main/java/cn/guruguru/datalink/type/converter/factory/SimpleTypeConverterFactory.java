package cn.guruguru.datalink.type.converter.factory;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.type.converter.DataTypeConverter;
import cn.guruguru.datalink.type.converter.FlinkDataTypeConverter;
import cn.guruguru.datalink.type.converter.SparkDataTypeConverter;

/**
 * Simple Converter Factory for creating a {@link
 * cn.guruguru.datalink.type.converter.DataTypeConverter}
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
     */
    public static DataTypeConverter<String> createInstance(String engineType) {
        switch (engineType) {
            case "Spark":
                return new SparkDataTypeConverter();
            case "Flink":
                return new FlinkDataTypeConverter();
            default:
                throw new UnsupportedEngineException("Unsupported engine:" + engineType);
        }
    }

    public static DataTypeConverter<String> of(String engineType) {
        return createInstance(engineType);
    }
}
