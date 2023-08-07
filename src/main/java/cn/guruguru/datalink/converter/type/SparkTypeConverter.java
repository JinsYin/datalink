package cn.guruguru.datalink.converter.type;

import cn.guruguru.datalink.converter.TypeConverter;
import cn.guruguru.datalink.protocol.field.FieldFormat;

public class SparkTypeConverter implements TypeConverter<FieldFormat> {

    @Override
    public FieldFormat toEngineType(String nodeType, FieldFormat fieldFormat) {
        throw new UnsupportedOperationException("Spark engine not supported");
    }
}
