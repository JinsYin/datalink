package cn.guruguru.datalink.converter.type;

import cn.guruguru.datalink.converter.TypeConverter;
import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.node.ExtractNode;

public class SparkTypeConverter implements TypeConverter {

    @Override
    public FieldFormat toEngineType(ExtractNode extractNode, FieldFormat fieldFormat) {
        throw new UnsupportedOperationException("Spark engine not supported");
    }
}
