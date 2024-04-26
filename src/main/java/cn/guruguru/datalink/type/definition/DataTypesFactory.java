package cn.guruguru.datalink.type.definition;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.parser.EngineType;

/**
 * Simple Factory for data types
 */
public class DataTypesFactory {
    public static DataTypes of(EngineType engineType) {
        switch (engineType) {
            case FLINK_SQL:
                return new FlinkDataTypes();
            case SPARK_SQL:
                return new SparkDataTypes();
            default:
                throw new UnsupportedEngineException("Unsupported engine: " + engineType);
        }
    }
}
