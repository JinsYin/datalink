package cn.guruguru.datalink.type.converter;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.protocol.field.DataType;

public class SparkDataTypeConverter implements DataTypeConverter<DataType> {

    @Override
    public DataType toEngineType(String nodeType, DataType dataType) {
        throw new UnsupportedEngineException("Spark engine not supported");
    }
}
