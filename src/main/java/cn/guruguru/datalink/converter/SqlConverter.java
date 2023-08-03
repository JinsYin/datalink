package cn.guruguru.datalink.converter;

import java.io.Serializable;

public interface SqlConverter extends Serializable {

    /**
     * Convert data retrieved from data source DDL to engine DDL
     *
     * @param sourceType data source type
     * @param ddl DDL SQL from Data Source
     */
    String toEngineDdl(String sourceType, String ddl);
}
