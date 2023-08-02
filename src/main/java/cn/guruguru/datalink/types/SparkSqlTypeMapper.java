package cn.guruguru.datalink.types;

import cn.guruguru.datalink.formats.FieldFormat;
import cn.guruguru.datalink.protocol.node.ExtractNode;

public class SparkSqlTypeMapper implements TypeMapper {
    @Override
    public String deriveEngineSql(ExtractNode extractNode, String ddl) {
        throw new UnsupportedOperationException("Spark engine not supported");
    }

    @Override
    public FieldFormat deriveEngineType(ExtractNode extractNode, FieldFormat fieldFormat) {
        throw new UnsupportedOperationException("Spark engine not supported");
    }
}
