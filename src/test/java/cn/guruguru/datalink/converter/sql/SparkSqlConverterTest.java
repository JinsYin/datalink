package cn.guruguru.datalink.converter.sql;

import cn.guruguru.datalink.converter.enums.JdbcDialect;
import org.junit.Test;

public class SparkSqlConverterTest {

    @Test
    public void testConvertToEngineDDL() {
        String createSQL =
            "CREATE TABLE ADM_BDPP.SPARK_PARAM ("
                + "ID NUMBER(20,0) NOT NULL, " // NUMBER 类型不支持
                + "APP_NAME VARCHAR(80), "
                + "SRC_ID NUMBER(20,0), " // VARCHAR2 类型不支持
                + "TGT_ID NUMBER(20,0), "
                + "APP_FUNC VARCHAR2(6),"
                + "LASTUPDATEDDT TIMESTAMP(6)"
                + ")";
        SparkSqlConverter sparkSqlConverter = new SparkSqlConverter();
        sparkSqlConverter.toEngineDDL(JdbcDialect.Oracle, "p1_catalog1", null, createSQL);
    }
}
