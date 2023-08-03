package cn.guruguru.datalink.converter;

import cn.guruguru.datalink.protocol.field.FieldFormat;
import cn.guruguru.datalink.protocol.node.ExtractNode;

public class SparkSqlTypeConverter implements TypeConverter {
    @Override
    public String deriveEngineSql(ExtractNode extractNode, String ddl) {
        throw new UnsupportedOperationException("Spark engine not supported");
    }

    @Override
    public FieldFormat deriveEngineType(ExtractNode extractNode, FieldFormat fieldFormat) {
        throw new UnsupportedOperationException("Spark engine not supported");
    }
}
