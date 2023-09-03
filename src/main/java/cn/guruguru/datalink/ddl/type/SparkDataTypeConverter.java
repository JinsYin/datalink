package cn.guruguru.datalink.ddl.type;

import cn.guruguru.datalink.exception.UnsupportedEngineException;
import cn.guruguru.datalink.protocol.field.FieldFormat;

public class SparkDataTypeConverter implements DataTypeConverter<FieldFormat> {

    @Override
    public FieldFormat toEngineType(String nodeType, FieldFormat fieldFormat) {
        throw new UnsupportedEngineException("Spark engine not supported");
    }
}
