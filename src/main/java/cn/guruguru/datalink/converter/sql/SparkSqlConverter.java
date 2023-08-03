package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.SqlConverter;

public class SparkSqlConverter implements SqlConverter {

    @Override
    public String toEngineDdl(String sourceType, String ddl) {
        throw new UnsupportedOperationException("Spark engine not supported");
    }
}
