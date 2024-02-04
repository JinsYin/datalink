package cn.guruguru.datalink.type.converter;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.protocol.field.DataType;

public class SparkDataTypeConverter implements DataTypeConverter<String> { // DataTypeConverter<DataType>

    private static final long serialVersionUID = 3025705873795163603L;

    @Override
    public String toEngineType(String nodeType, DataType dataType) {
        throw new UnsupportedEngineException("Spark engine not supported");
    }
}
